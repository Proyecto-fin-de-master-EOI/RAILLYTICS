"""Publicación de cada predicción en Gold (Parquet), para consultarla desde Superset con DuckDB.

Tabla `fact_prediccion_demanda`: un Parquet por ejecución (`<run_id>.parquet`), así el histórico crece sin reescribir
nada y se pueden comparar versiones del prompt o del modelo. Grano: día × ejecución. Superset la lee con
`read_parquet('s3://raillytics-gold/fact_prediccion_demanda/*.parquet')`. El CSV sigue siendo el entregable principal; esta
tabla añade el contexto del calendario (día de la semana, festivo, eventos) y marca si la predicción usó datos sintéticos.
GoldBuilderApp no la toca: solo reconstruye sus propias tablas.
"""
from __future__ import annotations

from datetime import date

import duckdb
import pandas as pd

from raillytics.prediccion.calendario import Calendario
from raillytics.prediccion.salida import contexto_por_dia
from raillytics.utils.lake import LakeLayout, ensure_parent_dir

GOLD_TABLA = "fact_prediccion_demanda"
COLUMNAS_GOLD = (
    "run_id", "generado_en", "trimestre", "corredor", "modelo", "version_prompt", "datos_sinteticos", "total_esperado",
    "fecha", "dia_semana", "festivo", "eventos", "viajeros_previstos", "indice", "motivo",
    "vispera", "puente", "regreso", "junto_a_evento", "exceso",
)


def construir_gold(df: pd.DataFrame, calendario: Calendario, *, total_esperado: int, datos_sinteticos: bool) -> pd.DataFrame:
    """El DataFrame del CSV más el calendario de cada día, con los tipos de la tabla Gold.

    Añade las marcas de víspera, puente, regreso y día junto a un evento y el `exceso` sobre un día corriente
    (`contexto_por_dia`): lo mismo que imprime el resumen de coherencia, pero comparable entre ejecuciones en Superset.
    """
    cal = calendario.dias
    por_fecha = {
        d.isoformat(): (dia, festivo or None, eventos or None)
        for d, dia, festivo, eventos in zip(cal["fecha"], cal["dia_semana"], cal["festivo"], cal["eventos"])
    }
    contexto = contexto_por_dia(df, calendario).set_index("fecha").reindex(df["fecha"])
    return pd.DataFrame(
        {
            "run_id": df["run_id"],
            "generado_en": pd.to_datetime(df["generado_en"].str.rstrip("Z")),
            "trimestre": df["trimestre"],
            "corredor": df["corredor"],
            "modelo": df["modelo"],
            "version_prompt": df["version_prompt"],
            "datos_sinteticos": bool(datos_sinteticos),
            "total_esperado": int(total_esperado),
            "fecha": [date.fromisoformat(f) for f in df["fecha"]],
            "dia_semana": [por_fecha[f][0] for f in df["fecha"]],
            "festivo": [por_fecha[f][1] for f in df["fecha"]],
            "eventos": [por_fecha[f][2] for f in df["fecha"]],
            "viajeros_previstos": df["viajeros_previstos"].astype("int64"),
            "indice": df["indice"],
            "motivo": df["motivo"],
            **{marca: contexto[marca].to_numpy(dtype=bool) for marca in ("vispera", "puente", "regreso", "junto_a_evento")},
            "exceso": contexto["exceso"].to_numpy(dtype=float),
        },
        columns=list(COLUMNAS_GOLD),
    )


def publicar_gold(con: duckdb.DuckDBPyConnection, layout: LakeLayout, gold: pd.DataFrame) -> str:
    """Escribe la ejecución como un Parquet en Gold (si se repite el mismo run_id, lo sobrescribe: no duplica filas)."""
    run_id = str(gold["run_id"].iloc[0])
    destino = f"{layout.gold_root}/{GOLD_TABLA}/{run_id}.parquet"
    ensure_parent_dir(destino)
    # pandas guarda fechas y horas como timestamps de nanosegundos; el contrato es DATE / TIMESTAMP, como lo escribiría Spark.
    # festivo y eventos van a None en los días sin dato: si lo son TODOS (trimestre sin festivos o sin eventos), DuckDB tipa la
    # columna como INTEGER, así que se fijan como texto para que todas las ejecuciones compartan esquema. El exceso que falta
    # (NaN en pandas) se guarda como NULL, no como NaN: AVG en SQL lo ignora en vez de devolver NaN.
    con.register("prediccion_gold", gold)
    try:
        con.execute(
            "COPY (SELECT * REPLACE (CAST(generado_en AS TIMESTAMP) AS generado_en, CAST(fecha AS DATE) AS fecha, "
            "CAST(festivo AS VARCHAR) AS festivo, CAST(eventos AS VARCHAR) AS eventos, "
            "CAST(CASE WHEN isnan(exceso) THEN NULL ELSE exceso END AS DOUBLE) AS exceso) "
            f"FROM prediccion_gold) TO '{destino}' (FORMAT PARQUET)"
        )
    finally:
        con.unregister("prediccion_gold")
    return destino
