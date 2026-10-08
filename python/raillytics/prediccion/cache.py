"""Caché de resultados del LLM: la misma entrada devuelve la misma respuesta al instante, sin llamar a Ollama.

La clave es el hash de TODO lo que determina la respuesta: el prompt completo (instrucciones, calendario, totales e
histórico), el modelo, la semilla, el contexto, la temperatura y el schema. Cambia cualquier cosa y la clave cambia, así que
no hay que invalidar nada a mano. Con temperatura 0 y semilla fija la respuesta ya es la que daría el modelo; la caché
solo se ahorra generarla otra vez (~2,5 min con 92 días).

Es de MEJOR ESFUERZO: si el directorio no se puede escribir o una entrada está corrupta, simplemente no hay acierto y se
llama al LLM. La caché nunca hace fallar una predicción. Los errores del LLM no se cachean.
"""
from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Sequence
from datetime import date, datetime, timezone
from pathlib import Path

from raillytics.prediccion.normalizar import IndiceDia
from raillytics.prediccion.ollama import (
    REINTENTOS,
    OllamaSettings,
    RespuestaInvalida,
    esquema_respuesta,
    validar_respuesta,
)
from raillytics.utils.fs import atomic_write_bytes

logger = logging.getLogger(__name__)

# Súbelo si cambia algo del código que altere el significado de una respuesta guardada (invalida toda la caché).
FORMATO = 1
TEMPERATURA = 0  # la del cliente de Ollama: forma parte de la clave


class CacheLLM:
    def __init__(self, directorio: Path) -> None:
        self.directorio = directorio

    def clave(self, prompt: str, settings: OllamaSettings, num_dias: int) -> str:
        entrada = {
            "formato": FORMATO,
            "prompt": prompt,
            "modelo": settings.modelo,
            "num_ctx": settings.num_ctx,
            "seed": settings.seed,
            "temperature": TEMPERATURA,
            "esquema": esquema_respuesta(num_dias),
        }
        return hashlib.sha256(json.dumps(entrada, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()

    def leer(self, clave: str, dias: Sequence[date]) -> list[IndiceDia] | None:
        """La respuesta guardada, revalidada contra el contrato actual; None si no hay o no es válida."""
        try:
            datos = json.loads((self.directorio / f"{clave}.json").read_text(encoding="utf-8"))
            return validar_respuesta(json.dumps({"dias": datos["dias"]}), dias)
        except (OSError, ValueError, KeyError, TypeError, RespuestaInvalida):
            return None

    def guardar(self, clave: str, indices: Sequence[IndiceDia], settings: OllamaSettings) -> None:
        contenido = {
            "formato": FORMATO,
            "clave": clave,
            "creado_en": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "modelo": settings.modelo,
            "num_dias": len(indices),
            "dias": [{"fecha": i.fecha.isoformat(), "motivo": i.motivo, "indice": i.indice} for i in indices],
        }
        try:
            self.directorio.mkdir(parents=True, exist_ok=True)
            atomic_write_bytes(self.directorio / f"{clave}.json", json.dumps(contenido, ensure_ascii=False).encode("utf-8"))
        except OSError as exc:
            logger.warning("no se pudo guardar en la caché %s: %s", self.directorio, exc)


class ClienteConCache:
    """Envuelve a un cliente del LLM: si la entrada ya se resolvió, devuelve la respuesta guardada sin llamarlo."""

    def __init__(self, cliente, cache: CacheLLM, imprimir: Callable[[str], None] = print) -> None:
        self._cliente = cliente
        self._cache = cache
        self._imprimir = imprimir
        self.settings = cliente.settings
        self.resultado_en_cache: bool | None = None  # None: aún no se ha pedido nada

    def comprobar(self) -> None:
        """Diferido: Ollama solo hace falta si no hay acierto, y se comprueba entonces."""

    def generar_indices(self, prompt: str, dias: Sequence[date], reintentos: int = REINTENTOS) -> list[IndiceDia]:
        clave = self._cache.clave(prompt, self.settings, len(dias))
        guardado = self._cache.leer(clave, dias)
        if guardado is not None:
            self.resultado_en_cache = True
            self._imprimir(
                f"Resultado en caché ({clave[:12]}): no se llama a {self.settings.modelo}. Usa --sin-cache para regenerarlo"
            )
            return guardado
        self.resultado_en_cache = False
        self._cliente.comprobar()
        indices = self._cliente.generar_indices(prompt, dias, reintentos)
        self._cache.guardar(clave, indices, self.settings)
        return indices
