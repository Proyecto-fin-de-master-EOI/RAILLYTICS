"""Publicación de cada predicción en Gold (Parquet) para consultarla desde Superset."""
from datetime import date, datetime

import duckdb
import pandas as pd
import pytest

from raillytics.prediccion.calendario import construir_calendario
from raillytics.prediccion.normalizar import IndiceDia, normalizar_indices, repartir
from raillytics.prediccion.publicacion import COLUMNAS_GOLD, GOLD_TABLA, construir_gold, publicar_gold
from raillytics.prediccion.salida import construir_dataframe
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.lake import LakeLayout, connect

T4 = Trimestre(2026, 4)
DIAS = T4.dias()
AHORA = datetime(2026, 10, 1, 16, 51, 0)


def _calendario():
    return construir_calendario(
        T4,
        pd.DataFrame([(date(2026, 12, 25), "Navidad")], columns=["fecha", "nombre"]),
        pd.DataFrame([(date(2026, 11, 29), "Partido de liga", "MAD")], columns=["fecha", "descripcion", "ciudad"]),
        pd.DataFrame([], columns=["fecha", "ciudad", "temperatura_media", "precipitacion_mm"]),
    )


def _df(run_id="run-1"):
    valores = [1.0 + 0.1 * (i % 3) for i in range(len(DIAS))]
    indices = [IndiceDia(d, v, f"motivo {d.day}") for d, v in zip(DIAS, valores)]
    return construir_dataframe(
        DIAS, indices, repartir(valores, 1_320_000), normalizar_indices(valores),
        trimestre=T4, modelo="mistral-nemo", version_prompt="demanda_v2", run_id=run_id, generado_en=AHORA,
    )


def test_construir_gold_tiene_las_columnas_del_contrato_y_las_marcas_del_calendario():
    gold = construir_gold(_df(), _calendario(), total_esperado=1_320_000, datos_sinteticos=True)

    assert tuple(gold.columns) == COLUMNAS_GOLD and len(gold) == 92
    assert set(gold["total_esperado"]) == {1_320_000} and set(gold["datos_sinteticos"]) == {True}
    navidad = gold[gold["fecha"] == date(2026, 12, 25)].iloc[0]
    assert (navidad["dia_semana"], navidad["festivo"]) == ("vie", "Navidad") and pd.isna(navidad["eventos"])
    partido = gold[gold["fecha"] == date(2026, 11, 29)].iloc[0]
    assert partido["eventos"] == "Partido de liga (MAD)" and pd.isna(partido["festivo"])
    assert gold["viajeros_previstos"].sum() == 1_320_000


def test_publicar_gold_escribe_un_parquet_por_ejecucion_con_tipos_de_columna_reales(tmp_path):
    layout = LakeLayout(silver_root=(tmp_path / "silver").as_posix(), gold_root=(tmp_path / "gold").as_posix())
    con = connect()

    ruta = publicar_gold(con, layout, construir_gold(_df("run-1"), _calendario(), total_esperado=1_320_000, datos_sinteticos=False))
    publicar_gold(con, layout, construir_gold(_df("run-2"), _calendario(), total_esperado=1_320_000, datos_sinteticos=False))

    assert ruta == f"{layout.gold_root}/{GOLD_TABLA}/run-1.parquet"
    glob = layout.gold_glob(GOLD_TABLA)
    tipos = dict(duckdb.connect().execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}')").fetchall()[i][:2] for i in range(len(COLUMNAS_GOLD)))
    assert tipos["fecha"] == "DATE" and tipos["generado_en"] == "TIMESTAMP" and tipos["datos_sinteticos"] == "BOOLEAN"
    assert tipos["viajeros_previstos"] == "BIGINT" and tipos["indice"] == "DOUBLE"
    assert duckdb.connect().execute(f"SELECT count(*), count(DISTINCT run_id) FROM read_parquet('{glob}')").fetchone() == (184, 2)


def test_republicar_la_misma_ejecucion_no_duplica_filas(tmp_path):
    layout = LakeLayout(silver_root=(tmp_path / "silver").as_posix(), gold_root=(tmp_path / "gold").as_posix())
    con = connect()
    gold = construir_gold(_df("run-1"), _calendario(), total_esperado=1_320_000, datos_sinteticos=False)

    publicar_gold(con, layout, gold)
    publicar_gold(con, layout, gold)

    assert duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{layout.gold_glob(GOLD_TABLA)}')").fetchone() == (92,)
