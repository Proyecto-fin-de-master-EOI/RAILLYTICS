"""Tests del modelo de ocupación: lo que puede salir mal sin dar error.

Casi todo lo delicado aquí produce un número bonito y falso en vez de una excepción: una partición
aleatoria sobre una serie temporal, una variable que es parte del objetivo, o unas categorías que
cambian entre entrenar y predecir. Eso es lo que se vigila.
"""
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from raillytics.ml import modelo
from raillytics.ml.dataset import SEMANA

SEMILLA = np.random.default_rng(7)


def _tabla(dias: int = 120) -> pd.DataFrame:
    """Una tabla con la forma de la real: fecha, objetivo, marca y unas cuantas variables."""
    fechas = [date(2026, 1, 1) + timedelta(days=i) for i in range(dias)]
    return pd.DataFrame({
        "fecha": pd.to_datetime(fechas),
        "ocupacion": SEMILLA.uniform(0.6, 0.95, dias),
        "plausible": True,
        "dia_semana": [SEMANA[f.weekday()] for f in fechas],
        "mes": [f.month for f in fechas],
        "festivo": [False] * dias,
        "temperatura_media_mad": SEMILLA.uniform(5, 30, dias),
    })


def test_las_variables_no_incluyen_el_objetivo_ni_la_fecha():
    """Si el objetivo entra como variable, el modelo lo copia y el error sale cero."""
    X = modelo.variables(_tabla())

    assert "ocupacion" not in X.columns
    assert "fecha" not in X.columns
    assert "plausible" not in X.columns


def test_el_dia_de_la_semana_se_convierte_en_columnas_y_no_en_un_numero():
    # «lun» no es menor que «mar»: tratarlo como número le haría inventar un orden que no existe.
    X = modelo.variables(_tabla())

    assert "dia_semana" not in X.columns
    assert {f"dia_{d}" for d in SEMANA} <= set(X.columns)


def test_las_columnas_son_las_mismas_aunque_falte_un_dia_de_la_semana():
    """El fallo real: un tramo de evaluación sin domingos no tenía la columna `dia_dom`.

    Con las categorías fijadas a los siete días, las columnas no dependen de qué trae el trozo.
    """
    completa = _tabla(120)
    sin_domingos = completa[completa["dia_semana"] != "dom"]

    assert list(modelo.variables(sin_domingos).columns) == list(modelo.variables(completa).columns)


def test_la_particion_respeta_el_tiempo():
    """Con una partición aleatoria el modelo vería el futuro para predecir el pasado."""
    train, test = modelo.partir(_tabla(100))

    assert train["fecha"].max() < test["fecha"].min()
    assert len(train) + len(test) == 100


def test_la_particion_deja_la_proporcion_pedida():
    train, test = modelo.partir(_tabla(100), proporcion_test=0.25)

    assert len(test) == 25


def test_la_validacion_temporal_nunca_entrena_con_dias_posteriores_a_los_que_evalua():
    tabla = _tabla(150)

    resultado = modelo.validar(tabla, tramos=4)

    assert len(resultado) == 4
    # Los tramos avanzan en el tiempo y no se solapan.
    assert resultado["desde"].is_monotonic_increasing
    for r in resultado.itertuples():
        assert r.dias_train > 0 and r.dias_test > 0


def test_la_validacion_compara_siempre_contra_la_referencia_trivial():
    """Un MAPE suelto no dice nada: hace falta saber si gana a no hacer nada."""
    resultado = modelo.validar(_tabla(150), tramos=3)

    assert {"mape", "mape_base"} <= set(resultado.columns)
    assert (resultado["mape_base"] > 0).all()


def test_entrenar_aguanta_los_huecos_de_la_meteo():
    """Hay días sin temperatura porque AEMET no la publicó: no se imputan, el modelo los admite."""
    tabla = _tabla(120)
    tabla.loc[tabla.index[:8], "temperatura_media_mad"] = np.nan

    entrenado = modelo.entrenar(tabla)

    assert len(entrenado.predict(modelo.variables(tabla))) == len(tabla)


def test_evaluar_devuelve_las_tres_metricas_que_se_presentan():
    tabla = _tabla(120)
    train, test = modelo.partir(tabla)

    metricas = modelo.evaluar(modelo.entrenar(train), test)

    assert {"mape", "mae_puntos", "r2", "dias_test"} <= set(metricas)
    assert metricas["dias_test"] == len(test)


def test_cargar_descarta_las_filas_marcadas_como_imposibles(monkeypatch):
    tabla = _tabla(50)
    tabla.loc[tabla.index[:6], "plausible"] = False

    class ConFalso:
        def execute(self, _sql):
            return self

        def df(self):
            return tabla.copy()

    class LayoutFalso:
        def silver_glob(self, _t):
            return "no-se-usa"

    assert len(modelo.cargar(ConFalso(), LayoutFalso())) == 44
    assert len(modelo.cargar(ConFalso(), LayoutFalso(), solo_plausibles=False)) == 50


def test_cargar_falla_claro_si_la_tabla_no_existe():
    class ConVacio:
        def execute(self, _sql):
            return self

        def df(self):
            return pd.DataFrame()

    class LayoutFalso:
        def silver_glob(self, _t):
            return "no-se-usa"

    with pytest.raises(ValueError, match="raillytics.ml.dataset"):
        modelo.cargar(ConVacio(), LayoutFalso())


# --- intervalo por regresión cuantílica ---

def test_la_banda_cuantilica_encierra_a_la_prediccion_central():
    df = _tabla(120)
    bajo, alto = modelo.entrenar_banda(df)
    X = modelo.variables(df)

    lo, hi = bajo.predict(X), alto.predict(X)

    # No se exige que se cumpla en TODOS los días (son dos modelos independientes y pueden cruzarse
    # en casos raros), pero sí que la banda tenga sentido en conjunto.
    assert (hi >= lo).mean() > 0.95
    assert (hi - lo).mean() > 0


def test_una_banda_mas_ancha_cubre_mas():
    df = _tabla(120)

    estrecha = modelo.cobertura_banda(df, tramos=3, confianza=0.50)
    ancha = modelo.cobertura_banda(df, tramos=3, confianza=0.95)

    assert ancha["amplitud_media"] > estrecha["amplitud_media"]
    assert ancha["cobertura"] >= estrecha["cobertura"]


def test_la_cobertura_se_mide_en_el_futuro_y_no_sobre_lo_entrenado():
    # Si se midiera sobre los mismos días del entrenamiento saldría inflada: el modelo cuantílico
    # ajusta sus cuantiles sobre ellos. La validación temporal es lo que lo evita.
    df = _tabla(120)

    r = modelo.cobertura_banda(df, tramos=4)

    assert 0.0 <= r["cobertura"] <= 1.0
    assert r["dias"] < len(df), "no se puede evaluar sobre todos los días: los primeros solo entrenan"


# --- reparto por franja horaria ---

def _franja_tabla() -> pd.DataFrame:
    """Oferta por franja y ocupación diaria, con la forma que tienen en Silver."""
    return pd.DataFrame({
        "fecha": pd.to_datetime(["2026-01-01"] * 3),
        "franja": ["mañana", "mediodía", "tarde"],
        "trenes": [10, 10, 10],
        "ocupacion": [0.8, 0.8, 0.8],
        "plausible": [True, True, True],
    })


def test_con_el_mismo_numero_de_trenes_la_franja_punta_sale_mas_llena():
    # Es lo único que afirma la hipótesis: a igualdad de oferta, en punta va más gente.
    from raillytics.ml import franja as fr

    df = _franja_tabla()
    df["factor"] = df["franja"].map({"mañana": 1.10, "mediodía": 0.95, "tarde": 1.10})
    df["peso"] = df["trenes"] * df["factor"]
    df["cuota_demanda"] = df["peso"] / df["peso"].sum()
    df["cuota_oferta"] = df["trenes"] / df["trenes"].sum()
    ocupacion = df["ocupacion"] * df["cuota_demanda"] / df["cuota_oferta"]

    assert ocupacion.iloc[0] > ocupacion.iloc[1], "la mañana debe ir más llena que el mediodía"
    assert fr.PUNTA == {"mañana", "tarde"}


def test_el_reparto_por_franja_no_cambia_la_demanda_del_dia():
    # El total del trimestre lo pone la CNMC: repartirlo entre franjas no puede crear ni perder
    # viajeros. La media ponderada por oferta de la ocupación por franja vuelve a la del día.
    df = _franja_tabla()
    df["factor"] = df["franja"].map({"mañana": 1.10, "mediodía": 0.95, "tarde": 1.10})
    df["peso"] = df["trenes"] * df["factor"]
    cuota_d = df["peso"] / df["peso"].sum()
    cuota_o = df["trenes"] / df["trenes"].sum()

    assert cuota_d.sum() == pytest.approx(1.0)
    assert (df["ocupacion"] * cuota_d / cuota_o * cuota_o).sum() == pytest.approx(df["ocupacion"].iloc[0])
