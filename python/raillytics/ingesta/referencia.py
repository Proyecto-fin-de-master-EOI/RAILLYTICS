"""Escribe en el lago las tablas de referencia que consume la predicción.

`festivos` y `meteo` son tablas DERIVADAS: las construye Python a partir de fuentes de la ingesta
(el calendario laboral del BOE y AEMET) y las lee la predicción. Van a Silver —como cualquier otra
tabla ya limpia y tipada— y no a una ruta del repositorio, por tres razones:

- se pueden redirigir con SILVER_ROOT, que es lo que permite probarlas sin tocar el proyecto;
- viven en MinIO como el resto del lago cuando el `.env` apunta a buckets;
- cada construcción queda registrada en la trazabilidad de cargas, igual que todo lo demás.
"""
from __future__ import annotations

import logging
from collections.abc import Mapping

import duckdb
import pandas as pd

from raillytics.utils.cargas import registrar_carga
from raillytics.utils.lake import LakeLayout, S3Settings, connect, ensure_parent_dir

logger = logging.getLogger(__name__)


def conexion(layout: LakeLayout, env: Mapping[str, str]) -> duckdb.DuckDBPyConnection:
    """Conexión DuckDB al lago: con MinIO si Silver y Gold son buckets, en memoria si son directorios."""
    return connect(S3Settings.from_env(env) if layout.uses_s3 else None)


def escribir(
    tabla: str,
    df: pd.DataFrame,
    parametros: Mapping[str, object],
    env: Mapping[str, str],
    origen: str,
) -> str:
    """Deja la tabla en su prefijo de Silver (sobrescribe: es idempotente) y registra la carga."""
    layout = LakeLayout.from_env(env)
    con = conexion(layout, env)
    destino = layout.silver_file(tabla)
    with registrar_carga(f"referencia_{tabla}", "silver", layout, con, parametros=parametros) as ejecucion:
        with ejecucion.tabla(tabla, origen=origen, destino=destino) as carga:
            ensure_parent_dir(destino)
            # pandas guarda las fechas como timestamps de nanosegundos y el contrato de la predicción
            # pide DATE, que es lo que escribiría Spark: se castea al copiar.
            con.register("referencia_frame", df)
            con.execute(
                "COPY (SELECT * REPLACE (CAST(fecha AS DATE) AS fecha) FROM referencia_frame) "
                f"TO '{destino}' (FORMAT PARQUET)"
            )
            con.unregister("referencia_frame")
            carga.filas = len(df)
    logger.info("%s: %d filas -> %s", tabla, len(df), destino)
    return destino
