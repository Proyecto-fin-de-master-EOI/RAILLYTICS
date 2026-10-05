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


def test_la_config_solo_lee_sinteticos_los_origenes_que_todavia_no_tienen_fuente_real():
    consultas = cargar_config(RAIZ / "config" / "prediccion.yml")

    # `trimestrales` sale del Silver de CNMC y `eventos` del CSV curado a mano: ya son reales.
    assert "muestra_" not in consultas["trimestrales"]
    assert "muestra_" not in consultas["eventos"]
    # festivos y meteo siguen pendientes: el BOE y AEMET ya están dados de alta en la ingesta, pero
    # todavía no existe la tabla de la que leerlos.
    assert all("muestra_" in consultas[origen] for origen in ("festivos", "meteo"))
