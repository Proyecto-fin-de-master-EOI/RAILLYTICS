"""Cliente de Ollama para la predicción: una llamada a /api/chat con la salida JSON forzada por schema.

El LLM devuelve un índice relativo por día. Aquí se valida de forma estricta (un registro por cada
día esperado, fecha ISO exacta, índice dentro de los límites) y, si falla, se reintenta reenviando
el prompt original más un aviso con el error. NO se reenvía la respuesta anterior: con 92 días
ocuparía ~4.000 tokens y desbordaría el contexto.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date

import requests

from raillytics.prediccion.normalizar import INDICE_MAX, INDICE_MIN, IndiceDia

logger = logging.getLogger(__name__)

REINTENTOS = 2
MAX_MOTIVO = 120
_FECHA_ISO = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


class OllamaError(Exception):
    """Fallo al obtener la predicción del LLM."""


class OllamaNoDisponible(OllamaError):
    """No hay conexión con el servidor de Ollama."""


class ModeloNoDescargado(OllamaError):
    """El modelo configurado no está en el servidor."""


class ContextoInsuficiente(OllamaError):
    """Prompt + respuesta no caben en `num_ctx`: Ollama truncó el prompt o cortó la respuesta."""


class RespuestaInvalida(OllamaError):
    """La respuesta del LLM no cumple el contrato (JSON, fechas, índices)."""


@dataclass(frozen=True)
class OllamaSettings:
    url: str = "http://localhost:11435"
    modelo: str = "mistral-nemo"
    num_ctx: int = 12288
    timeout_s: float = 900.0
    seed: int = 42

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> OllamaSettings:
        puerto = env.get("OLLAMA_PORT") or "11435"
        return cls(
            url=(env.get("OLLAMA_URL") or f"http://localhost:{puerto}").rstrip("/"),
            modelo=env.get("OLLAMA_MODEL") or cls.modelo,
            num_ctx=int(env.get("OLLAMA_NUM_CTX") or cls.num_ctx),
            timeout_s=float(env.get("OLLAMA_TIMEOUT_S") or cls.timeout_s),
            seed=int(env.get("OLLAMA_SEED") or cls.seed),
        )


def esquema_respuesta(n_dias: int) -> dict:
    """JSON schema de la respuesta. Ojo: Ollama 0.13.3 no admite '\\d' en un pattern (usar [0-9])."""
    return {
        "type": "object",
        "properties": {
            "dias": {
                "type": "array",
                "minItems": n_dias,
                "maxItems": n_dias,
                "items": {
                    "type": "object",
                    # El orden importa: Ollama fuerza las claves en el orden del schema. El motivo va ANTES del índice
                    # para que el modelo razone (día de la semana y causa) antes de comprometerse con un número.
                    "properties": {
                        "fecha": {"type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"},
                        "motivo": {"type": "string", "maxLength": MAX_MOTIVO},
                        "indice": {"type": "number", "minimum": INDICE_MIN, "maximum": INDICE_MAX},
                    },
                    "required": ["fecha", "motivo", "indice"],
                },
            }
        },
        "required": ["dias"],
    }


def _limpiar_motivo(texto: str) -> str:
    return " ".join(texto.split())[:MAX_MOTIVO]


def _validar(contenido: str, dias: Sequence[date]) -> list[IndiceDia]:
    try:
        datos = json.loads(contenido)
    except json.JSONDecodeError as exc:
        raise RespuestaInvalida(f"la respuesta no es JSON válido ({exc})") from exc
    entradas = datos.get("dias") if isinstance(datos, dict) else None
    if not isinstance(entradas, list):
        raise RespuestaInvalida("falta la lista 'dias' en la respuesta")
    por_fecha: dict[date, IndiceDia] = {}
    duplicadas: list[str] = []
    for posicion, entrada in enumerate(entradas):
        if not isinstance(entrada, dict):
            raise RespuestaInvalida(f"la entrada {posicion} no es un objeto")
        texto_fecha = entrada.get("fecha")
        if not isinstance(texto_fecha, str) or not _FECHA_ISO.match(texto_fecha):
            raise RespuestaInvalida(f"fecha no ISO AAAA-MM-DD en la entrada {posicion}: {texto_fecha!r}")
        try:
            fecha = date.fromisoformat(texto_fecha)
        except ValueError as exc:
            raise RespuestaInvalida(f"fecha inexistente en la entrada {posicion}: {texto_fecha!r}") from exc
        indice = entrada.get("indice")
        if (
            isinstance(indice, bool)
            or not isinstance(indice, (int, float))
            or not math.isfinite(indice)
            or not INDICE_MIN <= indice <= INDICE_MAX
        ):
            raise RespuestaInvalida(
                f"índice no numérico o fuera de [{INDICE_MIN}, {INDICE_MAX}] para {texto_fecha}: {indice!r}"
            )
        motivo = entrada.get("motivo", "")
        if fecha in por_fecha:
            duplicadas.append(texto_fecha)
            continue
        por_fecha[fecha] = IndiceDia(fecha, float(indice), _limpiar_motivo(motivo if isinstance(motivo, str) else ""))
    esperadas = set(dias)
    faltan = sorted(esperadas - set(por_fecha))
    sobran = sorted(set(por_fecha) - esperadas)
    problemas = []
    if faltan:
        problemas.append(f"faltan {len(faltan)} días: {', '.join(d.isoformat() for d in faltan[:5])}")
    if sobran:
        problemas.append(f"sobran {len(sobran)} días ajenos al periodo: {', '.join(d.isoformat() for d in sobran[:5])}")
    if duplicadas:
        problemas.append(f"hay fechas duplicadas: {', '.join(sorted(set(duplicadas))[:5])}")
    if problemas:
        raise RespuestaInvalida("; ".join(problemas))
    return [por_fecha[d] for d in dias]


class OllamaClient:
    def __init__(self, settings: OllamaSettings, session: requests.Session | None = None) -> None:
        self.settings = settings
        self._http = session or requests.Session()

    def comprobar(self) -> None:
        """Falla con instrucciones si Ollama no responde o el modelo no está descargado."""
        s = self.settings
        try:
            respuesta = self._http.get(f"{s.url}/api/tags", timeout=10)
            respuesta.raise_for_status()
            nombres = {m.get("name") for m in respuesta.json().get("models", [])}
        except requests.RequestException as exc:
            raise OllamaNoDisponible(
                f"no se puede conectar con Ollama en {s.url} ({exc}). "
                "Levántalo con: make llm-up (o docker compose --profile llm up -d)"
            ) from exc
        if s.modelo not in nombres and f"{s.modelo}:latest" not in nombres:
            raise ModeloNoDescargado(
                f"el modelo '{s.modelo}' no está en {s.url}. Descárgalo con: ollama pull {s.modelo} "
                "(o con el contenedor: make llm-up)"
            )

    def generar_indices(self, prompt: str, dias: Sequence[date], reintentos: int = REINTENTOS) -> list[IndiceDia]:
        esquema = esquema_respuesta(len(dias))
        aviso = ""
        ultimo_error = ""
        for intento in range(reintentos + 1):
            contenido = self._chat(prompt + aviso, esquema)
            try:
                return _validar(contenido, dias)
            except RespuestaInvalida as exc:
                ultimo_error = str(exc)
                logger.warning("respuesta inválida del LLM (intento %d de %d): %s", intento + 1, reintentos + 1, exc)
                aviso = (
                    f"\n\nAVISO: tu respuesta anterior fue rechazada ({exc}). "
                    "Devuelve de nuevo el JSON válido y completo, corrigiendo ese problema."
                )
        raise RespuestaInvalida(f"el LLM no devolvió una respuesta válida tras {reintentos + 1} intentos: {ultimo_error}")

    def _chat(self, contenido_usuario: str, esquema: dict) -> str:
        s = self.settings
        cuerpo = {
            "model": s.modelo,
            "messages": [{"role": "user", "content": contenido_usuario}],
            "stream": False,
            "format": esquema,
            "options": {"temperature": 0, "seed": s.seed, "num_ctx": s.num_ctx},
            # Medido en Ollama 0.13.3: por defecto, si la respuesta no cabe en num_ctx desplaza el contexto en silencio
            # (done_reason 'stop' con una respuesta degradada) y, si no cabe el prompt, lo trunca sin avisar.
            # Con estos dos campos avisa: done_reason 'length' y HTTP 400 'input length exceeds the context length'.
            "shift": False,
            "truncate": False,
        }
        try:
            respuesta = self._http.post(f"{s.url}/api/chat", json=cuerpo, timeout=s.timeout_s)
        except requests.ConnectionError as exc:
            raise OllamaNoDisponible(
                f"se perdió la conexión con Ollama en {s.url} ({exc}). "
                "Levántalo con: make llm-up (o docker compose --profile llm up -d)"
            ) from exc
        except requests.Timeout as exc:
            raise OllamaError(
                f"Ollama no respondió en {s.timeout_s:.0f} s. Sube OLLAMA_TIMEOUT_S o usa un modelo más rápido"
            ) from exc
        if respuesta.status_code == 404:
            raise ModeloNoDescargado(f"Ollama no conoce el modelo '{s.modelo}'. Descárgalo con: ollama pull {s.modelo}")
        if respuesta.status_code == 400 and "context length" in respuesta.text:
            raise ContextoInsuficiente(
                f"el prompt no cabe en el contexto (num_ctx={s.num_ctx}). Sube OLLAMA_NUM_CTX (92 días necesitan ~10.000)"
            )
        if not respuesta.ok:
            raise OllamaError(f"Ollama respondió {respuesta.status_code}: {respuesta.text[:300]}")
        try:
            datos = respuesta.json()
            contenido = datos["message"]["content"]
        except (ValueError, KeyError, TypeError) as exc:
            raise RespuestaInvalida("la respuesta de Ollama no trae message.content") from exc
        if datos.get("done_reason") == "length":
            raise ContextoInsuficiente(
                f"la respuesta de {s.modelo} se cortó por falta de contexto (num_ctx={s.num_ctx}). "
                "Sube OLLAMA_NUM_CTX (92 días necesitan ~10.000)"
            )
        if datos.get("prompt_eval_count", 0) >= s.num_ctx:
            raise ContextoInsuficiente(
                f"el prompt llenó el contexto (num_ctx={s.num_ctx}) y Ollama lo habrá truncado. "
                "Sube OLLAMA_NUM_CTX (92 días necesitan ~10.000)"
            )
        if datos.get("prompt_eval_count", 0) + datos.get("eval_count", 0) >= s.num_ctx:
            # Red de seguridad para versiones de Ollama que ignoren shift=false.
            raise ContextoInsuficiente(
                f"prompt y respuesta llenaron el contexto (num_ctx={s.num_ctx}): Ollama habrá desplazado el contexto y "
                "la respuesta no es fiable. Sube OLLAMA_NUM_CTX (92 días necesitan ~10.000)"
            )
        return contenido
