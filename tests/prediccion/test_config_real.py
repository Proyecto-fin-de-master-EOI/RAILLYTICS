"""La consulta `trimestrales` real de config/prediccion.yml, ejecutada sobre un Silver de prueba (sin red ni MinIO)."""
from pathlib import Path

import duckdb

from raillytics.prediccion.entradas import cargar_config, resolver_origen
from raillytics.utils.lake import LakeLayout

RAIZ = Path(__file__).resolve().parents[2]


def test_trimestrales_suma_los_operadores_del_corredor_sin_el_total_ni_otros_corredores(tmp_path):
    silver = tmp_path / "silver" / "cnmc_trimestral"
    silver.mkdir(parents=True)
    duckdb.connect().execute(
        f"""COPY (SELECT * FROM (VALUES
              (2026, 1, 'Madrid-Barcelona', 'RENFE', 1000), (2026, 1, 'Madrid-Barcelona', 'IRYO', 500), (2026, 1, 'Madrid-Barcelona', 'TOTAL', NULL),
              (2026, 2, 'Madrid-Barcelona', 'RENFE', 1100), (2026, 2, 'Madrid-Sevilla', 'RENFE', 777)
            ) t(anio, trimestre, corredor, operador_id, viajeros))
            TO '{(silver / 'cnmc_trimestral.parquet').as_posix()}' (FORMAT PARQUET)"""
    )
    layout = LakeLayout(silver_root=(tmp_path / "silver").as_posix(), gold_root=(tmp_path / "gold").as_posix())
    sql = resolver_origen(cargar_config(RAIZ / "config" / "prediccion.yml")["trimestrales"], layout, {})

    assert duckdb.connect().execute(sql).fetchall() == [("2026-T1", 1500), ("2026-T2", 1100)]


def test_la_config_no_lee_ninguna_fuente_sintetica():
    """Los cuatro orígenes apuntan a datos reales; la muestra solo se usa para probar sin ellos.

    El aviso de datos sintéticos de la predicción se decide con esto mismo (`"muestra_" in sql`, ver
    servicio.py), así que si alguna consulta volviera a la muestra el aviso lo diría y este test
    también: `trimestrales` sale del Silver de CNMC, `festivos` del calendario del BOE, `meteo` de
    AEMET y `eventos` del fichero curado a mano.
    """
    consultas = cargar_config(RAIZ / "config" / "prediccion.yml")

    sinteticas = sorted(origen for origen, sql in consultas.items() if "muestra_" in sql)
    assert sinteticas == []
