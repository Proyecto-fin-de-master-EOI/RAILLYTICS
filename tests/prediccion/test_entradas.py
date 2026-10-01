from datetime import date
from pathlib import Path

import pytest

from raillytics.prediccion.entradas import (
    EntradaError,
    cargar_config,
    cargar_entradas,
    raiz_bronze,
    resolver_origen,
)
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.lake import LakeLayout, connect

RAIZ = Path(__file__).resolve().parents[2]

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


def _parquet(con, destino, select):
    destino.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({select}) TO '{destino.as_posix()}' (FORMAT PARQUET)")


@pytest.fixture
def lake(tmp_path):
    con = connect()
    silver = tmp_path / "silver"
    _parquet(con, silver / "demanda" / "demanda.parquet", """
        SELECT * FROM (VALUES
          (2025, 1, 'AVE-MAD-BCN', 600000), (2025, 1, 'AVE-MAD-BCN', 400000),
          (2025, 1, 'OTRO', 999), (2025, 2, 'AVE-MAD-BCN', 650000)
        ) t(anio, trim, corredor, viajeros)""")
    _parquet(con, silver / "festivos" / "festivos.parquet", """
        SELECT * FROM (VALUES (DATE '2026-12-25', 'Navidad'), (DATE '2026-12-08', 'Inmaculada')) t(fecha, nombre)""")
    _parquet(con, silver / "eventos" / "eventos.parquet", """
        SELECT * FROM (VALUES
          (DATE '2026-11-29', 'Partido de liga', 'MAD'), (DATE '2026-11-29', 'Concierto', NULL::VARCHAR)
        ) t(fecha, descripcion, ciudad)""")
    _parquet(con, silver / "meteo" / "meteo.parquet", """
        SELECT * FROM (VALUES
          (DATE '2025-12-01', 'MAD', 9.5, 0.4), (DATE '2025-12-02', 'MAD', 8.0, 2.0)
        ) t(fecha, ciudad, tmed, prec)""")
    layout = LakeLayout(silver_root=silver.as_posix(), gold_root=(tmp_path / "gold").as_posix())
    return layout, con


def test_carga_los_cuatro_origenes_con_el_contrato(lake):
    layout, con = lake

    entradas = cargar_entradas(CONSULTAS, layout, con, env={})

    assert list(entradas.trimestrales["trimestre"]) == ["2025-T1", "2025-T2"]
    assert list(entradas.trimestrales["viajeros"]) == [1_000_000, 650_000]  # ambos sentidos sumados, sin 'OTRO'
    assert str(entradas.trimestrales["viajeros"].dtype) == "int64"
    assert entradas.trimestrales_dict() == {Trimestre(2025, 1): 1_000_000, Trimestre(2025, 2): 650_000}
    assert sorted(entradas.festivos["fecha"]) == [date(2026, 12, 8), date(2026, 12, 25)]
    assert sorted(entradas.eventos["ciudad"]) == ["", "MAD"]  # NULL -> ""
    assert list(entradas.meteo.columns) == ["fecha", "ciudad", "temperatura_media", "precipitacion_mm"]


def test_la_ciudad_de_la_meteo_se_normaliza(lake):
    layout, con = lake
    consultas = dict(
        CONSULTAS,
        meteo="SELECT DATE '2025-12-01' AS fecha, ' bcn ' AS ciudad, 10.0 AS temperatura_media, 0.0 AS precipitacion_mm",
    )

    assert list(cargar_entradas(consultas, layout, con, env={}).meteo["ciudad"]) == ["BCN"]


def test_los_origenes_pueden_estar_vacios_salvo_que_el_nivel_los_necesite(lake):
    layout, con = lake
    consultas = dict(CONSULTAS, eventos="SELECT DATE '2026-01-01' AS fecha, 'x' AS descripcion, 'MAD' AS ciudad WHERE false")

    assert cargar_entradas(consultas, layout, con, env={}).eventos.empty


def test_resolver_origen_sustituye_los_marcadores(tmp_path):
    layout = LakeLayout(silver_root="/s", gold_root="/g")

    sql = "{bronze}/a {silver}/b {gold}/c"

    assert resolver_origen(sql, layout, {"BRONZE_ROOT": "/b"}) == "/b/a /s/b /g/c"
    assert resolver_origen(sql, layout, {}) == "s3://raillytics-bronze/a /s/b /g/c"


def test_raiz_bronze_sale_del_entorno():
    assert raiz_bronze({"BRONZE_ROOT": "/tmp/b"}) == "/tmp/b"
    assert raiz_bronze({"MINIO_BUCKET_BRONZE": "mi-bronze"}) == "s3://mi-bronze"
    assert raiz_bronze({}) == "s3://raillytics-bronze"


def test_un_origen_inexistente_nombra_la_fuente_y_la_ruta(lake):
    layout, con = lake
    consultas = dict(
        CONSULTAS, eventos="SELECT fecha, descripcion, ciudad FROM read_parquet('{silver}/sin_ingestar/*.parquet')"
    )

    with pytest.raises(EntradaError) as error:
        cargar_entradas(consultas, layout, con, env={})

    assert "origen 'eventos'" in str(error.value) and "sin_ingestar" in str(error.value)


def test_columnas_distintas_del_contrato_se_rechazan(lake):
    layout, con = lake
    consultas = dict(CONSULTAS, trimestrales="SELECT 2025 AS anio, 1 AS viajeros")

    with pytest.raises(EntradaError, match=r"exactamente las columnas \['trimestre', 'viajeros'\]"):
        cargar_entradas(consultas, layout, con, env={})


@pytest.mark.parametrize(
    "origen, sql, mensaje",
    [
        ("festivos", "SELECT DATE '2026-12-25' AS fecha, NULL::VARCHAR AS nombre", "nulos en columnas obligatorias"),
        (
            "festivos",
            "SELECT * FROM (VALUES (DATE '2026-12-25', 'A'), (DATE '2026-12-25', 'B')) t(fecha, nombre)",
            "duplicad",
        ),
        (
            "meteo",
            "SELECT DATE '2025-12-01' AS fecha, 'SEV' AS ciudad, 10.0 AS temperatura_media, 0.0 AS precipitacion_mm",
            "ciudades no soportadas",
        ),
        (
            "meteo",
            "SELECT * FROM (VALUES (DATE '2025-12-01', 'MAD', 1.0, 1.0), (DATE '2025-12-01', 'MAD', 2.0, 2.0)) "
            "t(fecha, ciudad, temperatura_media, precipitacion_mm)",
            "duplicad",
        ),
        ("trimestrales", "SELECT '2025-Q1' AS trimestre, 100 AS viajeros", "formato inválido"),
        ("trimestrales", "SELECT '2025-T1' AS trimestre, 100.5 AS viajeros", "entero"),
    ],
)
def test_datos_que_incumplen_el_contrato_fallan_cerrado(lake, origen, sql, mensaje):
    layout, con = lake

    with pytest.raises(EntradaError, match=mensaje) as error:
        cargar_entradas(dict(CONSULTAS, **{origen: sql}), layout, con, env={})

    assert f"origen '{origen}'" in str(error.value)


def test_cargar_config_devuelve_una_consulta_por_origen(tmp_path):
    fichero = tmp_path / "prediccion.yml"
    fichero.write_text(
        "origenes:\n"
        + "".join(f"  {nombre}:\n    sql: |\n      SELECT 1\n" for nombre in CONSULTAS),
        encoding="utf-8",
    )

    assert cargar_config(fichero) == {nombre: "SELECT 1\n" for nombre in CONSULTAS}


def test_cargar_config_pide_cada_origen(tmp_path):
    fichero = tmp_path / "prediccion.yml"
    fichero.write_text("origenes:\n  festivos:\n    sql: SELECT 1\n", encoding="utf-8")

    with pytest.raises(EntradaError, match="origenes.trimestrales.sql"):
        cargar_config(fichero)


def test_cargar_config_falla_si_no_existe(tmp_path):
    with pytest.raises(EntradaError, match="no existe"):
        cargar_config(tmp_path / "no_existe.yml")


def test_la_config_por_defecto_del_repo_define_los_cuatro_origenes():
    consultas = cargar_config(RAIZ / "config" / "prediccion.yml")

    assert set(consultas) == set(CONSULTAS)
    assert all("SELECT" in sql for sql in consultas.values())
