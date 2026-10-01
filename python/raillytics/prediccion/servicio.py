"""Orquestación de la predicción: entradas → nivel → prompt → LLM → gates → CSV, con trazabilidad.

Todo lo que puede fallar (leer el lake, calcular el nivel, el LLM, los gates, escribir) ocurre dentro
de `registrar_carga`, así que cualquier error queda registrado con `estado = error` y no se escribe
ningún CSV. `--solo-nivel` solo calcula e imprime: no llama al LLM ni registra nada.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Protocol

import duckdb
import pandas as pd

from raillytics.calidad.registro import exigir, registrar_calidad
from raillytics.prediccion.calendario import Calendario, construir_calendario
from raillytics.prediccion.entradas import Entradas, cargar_config, cargar_entradas, raiz_bronze
from raillytics.prediccion.gates import TABLA, evaluar_gates
from raillytics.prediccion.nivel import NivelEsperado, calcular_nivel, desviacion_relativa, miles
from raillytics.prediccion.normalizar import IndiceDia, normalizar_indices, repartir
from raillytics.prediccion.ollama import OllamaClient, OllamaSettings
from raillytics.prediccion.prompt import cargar_plantilla, construir_prompt
from raillytics.prediccion.salida import (
    CORREDOR,
    construir_dataframe,
    escribir_csv,
    formatear_resumen,
    resumen_coherencia,
)
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.cargas import registrar_carga
from raillytics.utils.lake import LakeLayout, S3Settings, connect, is_s3

logger = logging.getLogger(__name__)

PROCESO = "prediccion_demanda"
CAPA = "ml"
VERSION_PROMPT_POR_DEFECTO = "demanda_v2"


class ClienteLLM(Protocol):
    settings: OllamaSettings

    def comprobar(self) -> None: ...

    def generar_indices(self, prompt: str, dias: Sequence[date]) -> list[IndiceDia]: ...


@dataclass(frozen=True)
class Resultado:
    nivel: NivelEsperado | None
    total_esperado: int
    ruta: Path | None
    dataframe: pd.DataFrame | None
    resumen: str | None


def conectar(layout: LakeLayout, env: Mapping[str, str]) -> duckdb.DuckDBPyConnection:
    """Conexión DuckDB al lake: con MinIO si Bronze/Silver/Gold son buckets, en memoria si son directorios locales."""
    usa_s3 = layout.uses_s3 or is_s3(raiz_bronze(env))
    return connect(S3Settings.from_env(env) if usa_s3 else None)


def _preparar(
    trimestre: Trimestre,
    total_manual: int | None,
    env: Mapping[str, str],
    layout: LakeLayout,
    con: duckdb.DuckDBPyConnection,
    imprimir: Callable[[str], None],
) -> tuple[Entradas, dict[Trimestre, int], NivelEsperado | None, int]:
    """Lee los cuatro orígenes y fija el total esperado (calculado o manual)."""
    consultas = cargar_config(Path(env.get("PREDICCION_CONFIG") or "config/prediccion.yml"))
    if any("muestra_" in sql for sql in consultas.values()):
        imprimir(
            "AVISO: las consultas leen fuentes SINTÉTICAS (muestra_*): los resultados NO son reales. Cuando Airflow "
            "ingeste las fuentes reales, apunta config/prediccion.yml a ellas"
        )
    entradas = cargar_entradas(consultas, layout, con, env)
    publicados = entradas.trimestrales_dict()
    nivel = None
    if total_manual is None:
        nivel = calcular_nivel(publicados, trimestre)
        total = nivel.total
        imprimir(nivel.describir())
    else:
        total = total_manual
        imprimir(f"{trimestre}: total esperado fijado a mano: {miles(total)} viajeros")
    if trimestre in publicados:
        real = publicados[trimestre]
        imprimir(
            f"{trimestre} ya está publicado ({miles(real)} viajeros reales): "
            f"el total esperado se desvía {desviacion_relativa(total, real):+.2%}"
        )
    return entradas, publicados, nivel, total


def _prompt(
    trimestre: Trimestre,
    version_prompt: str,
    total: int,
    publicados: dict[Trimestre, int],
    entradas: Entradas,
    env: Mapping[str, str],
) -> tuple[str, Calendario]:
    """Calendario + plantilla + prompt final: lo que se muestra con --mostrar-prompt es lo que se envía."""
    calendario = construir_calendario(trimestre, entradas.festivos, entradas.eventos, entradas.meteo)
    plantilla = cargar_plantilla(version_prompt, Path(env.get("PROMPTS_DIR") or "config/prompts"))
    return construir_prompt(plantilla, trimestre, total, publicados, calendario, CORREDOR), calendario


def ejecutar(
    trimestre: Trimestre,
    *,
    version_prompt: str,
    total_manual: int | None,
    solo_nivel: bool,
    mostrar_prompt: bool = False,
    env: Mapping[str, str],
    layout: LakeLayout,
    con: duckdb.DuckDBPyConnection,
    cliente: ClienteLLM | None,
    ahora: datetime | None = None,
    imprimir: Callable[[str], None] = print,
) -> Resultado:
    if solo_nivel:
        _, _, nivel, total = _preparar(trimestre, total_manual, env, layout, con, imprimir)
        return Resultado(nivel, total, None, None, None)
    if mostrar_prompt:  # imprime el prompt y termina: ni se comprueba Ollama ni se gasta GPU ni se registra carga
        entradas, publicados, nivel, total = _preparar(trimestre, total_manual, env, layout, con, imprimir)
        prompt, _ = _prompt(trimestre, version_prompt, total, publicados, entradas, env)
        imprimir(prompt)
        return Resultado(nivel, total, None, None, None)
    if cliente is None:
        raise ValueError("hace falta un cliente del LLM salvo con solo_nivel o mostrar_prompt")

    ahora = ahora or datetime.now(timezone.utc).replace(tzinfo=None)
    s = cliente.settings
    parametros = {
        "trimestre": str(trimestre),
        "modelo": s.modelo,
        "version_prompt": version_prompt,
        "seed": s.seed,
        "num_ctx": s.num_ctx,
        "total_manual": total_manual,
    }
    raiz = Path(env.get("PREDICCIONES_ROOT") or "data/predicciones")
    with registrar_carga(PROCESO, CAPA, layout, con, parametros=parametros) as ejecucion:
        with ejecucion.tabla(TABLA, origen=f"{s.modelo} · {version_prompt}") as carga:
            entradas, publicados, nivel, total = _preparar(trimestre, total_manual, env, layout, con, imprimir)
            prompt, calendario = _prompt(trimestre, version_prompt, total, publicados, entradas, env)
            cliente.comprobar()
            dias = trimestre.dias()
            imprimir(f"Pidiendo a {s.modelo} el índice de {len(dias)} días (puede tardar varios minutos)...")
            indices = cliente.generar_indices(prompt, dias)
            valores = [i.indice for i in indices]
            df = construir_dataframe(
                dias, indices, repartir(valores, total), normalizar_indices(valores),
                trimestre=trimestre, modelo=s.modelo, version_prompt=version_prompt,
                run_id=ejecucion.run_id, generado_en=ahora,
            )
            resultados = evaluar_gates(df, dias, total, calendario.eventos_con_datos, valores)
            registrar_calidad(resultados, PROCESO, CAPA, ejecucion.run_id, layout, con)
            for r in resultados:
                if not r.pasa and not r.bloquea:
                    imprimir(f"AVISO {r.gate}: {r.detalle}")
            exigir(resultados)
            ruta = escribir_csv(df, raiz, trimestre, version_prompt, ahora)
            carga.destino = str(ruta)
            carga.filas = len(df)
            carga.bytes = ruta.stat().st_size
    resumen = formatear_resumen(resumen_coherencia(df, calendario))
    imprimir(f"CSV escrito: {ruta}")
    imprimir(resumen)
    return Resultado(nivel, total, ruta, df, resumen)


def predecir_desde_entorno(
    trimestre: str | None,
    hoy: date,
    env: Mapping[str, str],
    *,
    version_prompt: str | None = None,
    imprimir: Callable[[str], None] = print,
) -> Resultado:
    """Punto de entrada del DAG de Airflow: el lake, Ollama y las rutas salen del entorno del contenedor.

    Sin `trimestre` se predice el trimestre en curso según `hoy`; sin `version_prompt`, el de PRED_PROMPT o el de por defecto.
    Los errores se propagan como excepciones (Airflow marca la tarea en rojo).
    """
    objetivo = Trimestre.parse(trimestre) if trimestre else Trimestre.de_fecha(hoy)
    layout = LakeLayout.from_env(env)
    return ejecutar(
        objetivo,
        version_prompt=version_prompt or env.get("PRED_PROMPT") or VERSION_PROMPT_POR_DEFECTO,
        total_manual=None,
        solo_nivel=False,
        env=env,
        layout=layout,
        con=conectar(layout, env),
        cliente=OllamaClient(OllamaSettings.from_env(env)),
        imprimir=imprimir,
    )
