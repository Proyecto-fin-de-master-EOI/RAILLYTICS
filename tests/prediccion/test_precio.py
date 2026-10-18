"""Tests del precio recomendado: lo que puede salir mal dando un número razonable.

Casi nada de lo delicado aquí revienta. Un signo invertido en la elasticidad abarata los días llenos
y lo que sale sigue pareciendo una tabla de precios; una referencia contaminada por los días
imposibles desplaza todas las recomendaciones a la vez sin que ninguna llame la atención; y un
operador al que la CNMC deja de publicarle el precio puede heredar en silencio uno viejo. Eso es lo
que se vigila.
"""
from datetime import date, timedelta

import duckdb
import pandas as pd
import pytest

from raillytics.prediccion import precio
from raillytics.utils.lake import LakeLayout, ensure_parent_dir


@pytest.fixture
def layout(tmp_path) -> LakeLayout:
    return LakeLayout(silver_root=(tmp_path / "silver").as_posix(), gold_root=(tmp_path / "gold").as_posix())


def _escribir(con, destino, df):
    ensure_parent_dir(destino)
    con.register("tmp_frame", df)
    try:
        con.execute(f"COPY (SELECT * FROM tmp_frame) TO '{destino}' (FORMAT PARQUET)")
    finally:
        con.unregister("tmp_frame")


def _lago(con, layout, ocupaciones, precios, plausibles=None):
    """Escribe en el lago de pruebas la tabla de franja y la de precios de la CNMC."""
    dias = len(ocupaciones)
    franja = pd.DataFrame({
        "fecha": pd.to_datetime([date(2026, 1, 1) + timedelta(days=i) for i in range(dias)]),
        "franja": ["mañana"] * dias,
        "es_punta": [True] * dias,
        "ocupacion": ocupaciones,
        "plausible": plausibles if plausibles is not None else [True] * dias,
    })
    _escribir(con, f"{layout.gold_root}/fact_ocupacion_franja/fact_ocupacion_franja.parquet", franja)
    _escribir(con, layout.silver_file("cnmc_precio_trimestral"), pd.DataFrame(precios))


def _precios(operador="RENFE", valores=((2026, 1, 60.0), (2026, 2, 60.0))):
    return [
        {"anio": a, "trimestre": t, "trayecto": "Madrid-Barcelona", "empresa": operador,
         "operador_id": operador, "precio_eur": p}
        for a, t, p in valores
    ]


# --------------------------------------------------------------------------- la regla declarada

def test_elasticidad_positiva_no_se_acepta():
    """Con elasticidad positiva la regla abarataría los días llenos: es un error, no una opción."""
    with pytest.raises(ValueError, match="negativa"):
        precio.Regla(0.9, None)


def test_exponente_es_el_inverso_de_la_elasticidad():
    assert precio.Regla(-1.0, None).exponente == pytest.approx(1.0)
    assert precio.Regla(-0.5, None).exponente == pytest.approx(2.0)


def test_la_version_cambia_con_los_parametros():
    """Sin esto el ratio de aceptación no sería atribuible: no se sabría qué regla se aceptó."""
    base = precio.Regla(-1.0, None).version(0.73)
    assert base != precio.Regla(-0.9, None).version(0.73)
    assert base != precio.Regla(-1.0, None).version(0.80)
    assert base == precio.Regla(-1.0, None).version(0.73)


def test_referencia_fuera_de_rango_no_se_acepta(tmp_path):
    ruta = tmp_path / "reglas.yml"
    ruta.write_text("precio_recomendado:\n  elasticidad: -1.0\n  referencia_ocupacion: 1.4\n", encoding="utf-8")
    with pytest.raises(ValueError, match="fuera de"):
        precio.cargar_regla(ruta)


# --------------------------------------------------------------------------- la recomendación

def test_la_ocupacion_de_referencia_deja_el_precio_base(layout):
    """Un día que va como la media no cambia de precio: la regla es neutra, no sube por costumbre."""
    con = duckdb.connect()
    _lago(con, layout, [0.80, 0.80, 0.80], _precios())
    df = precio.construir(con, layout, precio.Regla(-1.0, None))

    assert df["ocupacion_referencia"].to_numpy() == pytest.approx(0.80)
    assert df["factor"].to_numpy() == pytest.approx(1.0)
    assert df["precio_recomendado_eur"].to_numpy() == pytest.approx(60.0)


def test_mas_ocupacion_mas_precio(layout):
    """La dirección es lo primero que rompería un signo invertido, y seguiría pareciendo una tabla."""
    con = duckdb.connect()
    _lago(con, layout, [0.60, 0.70, 0.80, 0.90], _precios())
    df = precio.construir(con, layout, precio.Regla(-1.0, None)).sort_values("ocupacion")

    assert df["precio_sin_acotar_eur"].is_monotonic_increasing
    assert df["precio_sin_acotar_eur"].iloc[0] < 60.0 < df["precio_sin_acotar_eur"].iloc[-1]


def test_una_elasticidad_menor_mueve_mas_el_precio(layout):
    """Con demanda más rígida hace falta más precio para el mismo efecto: |ε| menor, factor mayor."""
    con = duckdb.connect()
    _lago(con, layout, [0.70, 0.90], _precios())
    suave = precio.construir(con, layout, precio.Regla(-1.0, None))
    rigida = precio.construir(con, layout, precio.Regla(-0.5, None))

    alto = suave["ocupacion"].idxmax()
    assert rigida["factor"].iloc[alto] > suave["factor"].iloc[alto] > 1.0


def test_no_se_sale_del_recorrido_observado(layout):
    """El recorrido real del operador es el tope: la regla no propone precios que nadie ha puesto."""
    con = duckdb.connect()
    # Recorrido observado 55-65 € y una ocupación que pide bastante menos y bastante más que eso.
    _lago(con, layout, [0.30, 0.70, 0.95], _precios(valores=((2026, 1, 55.0), (2026, 2, 65.0))))
    df = precio.construir(con, layout, precio.Regla(-1.0, 0.70))

    assert (df["precio_recomendado_eur"] >= df["precio_min_eur"]).all()
    assert (df["precio_recomendado_eur"] <= df["precio_max_eur"]).all()
    # `recortado` tiene que marcar exactamente las que se tocaron, que es el KPI que se publica.
    tocadas = df["precio_sin_acotar_eur"] != df["precio_recomendado_eur"]
    assert df["recortado"].tolist() == tocadas.tolist()
    assert df["en_rango_observado"].tolist() == (~tocadas).tolist()
    # Y la prueba vale algo: hay de las dos, recortadas y no recortadas.
    assert set(df["recortado"]) == {True, False}


def test_los_dias_imposibles_no_mueven_la_referencia(layout):
    """Un día con ocupación imposible arrastra el problema del denominador del NAP. Si entrara en la
    referencia subiría de golpe todas las recomendaciones del corredor."""
    con = duckdb.connect()
    _lago(con, layout, [0.70, 0.70, 0.70, 2.50], _precios(), plausibles=[True, True, True, False])
    df = precio.construir(con, layout, precio.Regla(-1.0, None))

    assert df["ocupacion_referencia"].iloc[0] == pytest.approx(0.70)
    # El día imposible sigue en la tabla, marcado: se señala, no se borra.
    assert (~df["plausible"]).sum() == 1


def test_sin_precio_publicado_no_hay_recomendacion(layout, caplog):
    """Le pasa a AVLO de verdad: la CNMC deja de desglosarlo en 2026. No se le puede arrastrar el
    precio del último trimestre que tuviera, porque ya no está vigente."""
    con = duckdb.connect()
    precios = _precios() + _precios("AVLO", valores=((2025, 3, 50.0), (2025, 4, 49.0)))
    _lago(con, layout, [0.70, 0.80], precios)

    with caplog.at_level("WARNING", logger="raillytics.prediccion.precio"):
        df = precio.construir(con, layout, precio.Regla(-1.0, None))

    assert set(df["operador_id"]) == {"RENFE"}
    assert "AVLO" in caplog.text


def test_el_precio_base_es_el_del_trimestre_del_dia(layout):
    """Si el precio base se tomara del operador sin mirar el trimestre, la recomendación de enero
    saldría de un precio de junio y nadie lo vería en la tabla."""
    con = duckdb.connect()
    # 95 días: cruzan de T1 a T2, con precios base distintos.
    _lago(con, layout, [0.75] * 95, _precios(valores=((2026, 1, 40.0), (2026, 2, 80.0))))
    df = precio.construir(con, layout, precio.Regla(-1.0, None))

    por_trimestre = df.groupby("trimestre")["precio_base_eur"].unique()
    assert por_trimestre[1].tolist() == [40.0]
    assert por_trimestre[2].tolist() == [80.0]
    assert len(df) == 95
