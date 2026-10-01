from types import SimpleNamespace

import pytest
import yaml

from raillytics.utils.lake import LakeLayout, connect

# Siete trimestres publicados: el nivel de 2026-T4 es 1.200.000 × (1.430.000 / 1.300.000) = 1.320.000.
CONSULTAS = {
    "trimestrales": (
        "SELECT concat(anio, '-T', trim) AS trimestre, sum(viajeros) AS viajeros "
        "FROM read_parquet('{silver}/demanda/*.parquet') WHERE corredor = 'AVE-MAD-BCN' GROUP BY 1 ORDER BY 1"
    ),
    "festivos": "SELECT fecha, nombre FROM read_parquet('{silver}/festivos/*.parquet')",
    "eventos": "SELECT fecha, descripcion, ciudad FROM read_parquet('{silver}/eventos/*.parquet')",
    "meteo": (
        "SELECT fecha, ciudad, tmed AS temperatura_media, prec AS precipitacion_mm "
        "FROM read_parquet('{silver}/meteo/*.parquet')"
    ),
}

PLANTILLA = (
    "{{corredor}} {{trimestre}} {{num_dias}} dias, total {{total_esperado}}\n{{historico}}\n{{nota_eventos}}\n{{calendario}}"
)


def _parquet(con, destino, select):
    destino.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({select}) TO '{destino.as_posix()}' (FORMAT PARQUET)")


@pytest.fixture
def entorno(tmp_path):
    con = connect()
    silver = tmp_path / "silver"
    _parquet(con, silver / "demanda" / "demanda.parquet", """
        SELECT * FROM (VALUES
          (2025, 1, 'AVE-MAD-BCN', 600000), (2025, 1, 'AVE-MAD-BCN', 400000),
          (2025, 2, 'AVE-MAD-BCN', 1100000), (2025, 3, 'AVE-MAD-BCN', 1300000),
          (2025, 4, 'AVE-MAD-BCN', 1200000), (2026, 1, 'AVE-MAD-BCN', 1050000),
          (2026, 2, 'AVE-MAD-BCN', 1210000), (2026, 3, 'AVE-MAD-BCN', 1430000),
          (2026, 3, 'OTRO', 5)
        ) t(anio, trim, corredor, viajeros)""")
    _parquet(con, silver / "festivos" / "festivos.parquet", """
        SELECT * FROM (VALUES (DATE '2026-12-25', 'Navidad'), (DATE '2026-12-08', 'Inmaculada')) t(fecha, nombre)""")
    _parquet(con, silver / "eventos" / "eventos.parquet", """
        SELECT * FROM (VALUES (DATE '2026-11-29', 'Partido de liga', 'MAD')) t(fecha, descripcion, ciudad)""")
    _parquet(con, silver / "meteo" / "meteo.parquet", """
        SELECT * FROM (VALUES
          (DATE '2025-12-01', 'MAD', 9.5, 0.4), (DATE '2025-12-01', 'BCN', 13.0, 1.0)
        ) t(fecha, ciudad, tmed, prec)""")
    config = tmp_path / "prediccion.yml"
    config.write_text(
        yaml.safe_dump({"origenes": {nombre: {"sql": sql} for nombre, sql in CONSULTAS.items()}}), encoding="utf-8"
    )
    prompts = tmp_path / "prompts"
    prompts.mkdir()
    (prompts / "demanda_v1.md").write_text(PLANTILLA, encoding="utf-8")
    salida = tmp_path / "salida"
    env = {
        "PREDICCION_CONFIG": str(config),
        "PROMPTS_DIR": str(prompts),
        "PREDICCIONES_ROOT": str(salida),
    }
    layout = LakeLayout(silver_root=silver.as_posix(), gold_root=(tmp_path / "gold").as_posix())
    return SimpleNamespace(env=env, layout=layout, con=con, salida=salida, config=config, silver=silver)
