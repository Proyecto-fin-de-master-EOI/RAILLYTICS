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
