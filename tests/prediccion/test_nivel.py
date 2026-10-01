import pytest

from raillytics.prediccion.nivel import NivelError, calcular_nivel, desviacion_relativa, miles
from raillytics.prediccion.trimestre import Trimestre

T = Trimestre.parse

HISTORICO = {
    T("2025-T1"): 1_000_000,
    T("2025-T2"): 1_100_000,
    T("2025-T3"): 1_300_000,
    T("2025-T4"): 1_200_000,
    T("2026-T1"): 1_050_000,
    T("2026-T2"): 1_210_000,
    T("2026-T3"): 1_430_000,
}


def test_miles_usa_el_separador_espanol():
    assert miles(1_320_000) == "1.320.000"
    assert miles(950) == "950"


def test_nivel_es_el_mismo_trimestre_del_anio_anterior_por_el_crecimiento_interanual_del_ultimo():
    nivel = calcular_nivel(HISTORICO, T("2026-T4"))

    # 2025-T4 (1.200.000) × (2026-T3 / 2025-T3 = 1.430.000 / 1.300.000 = 1,10)
    assert nivel.total == 1_320_000 and isinstance(nivel.total, int)
    assert (nivel.base, nivel.ultimo, nivel.referencia) == (T("2025-T4"), T("2026-T3"), T("2025-T3"))
    assert nivel.crecimiento == pytest.approx(0.10)


def test_describir_explica_de_donde_sale_el_total():
    texto = calcular_nivel(HISTORICO, T("2026-T4")).describir()

    assert "2026-T4" in texto and "1.320.000" in texto and "2025-T4" in texto and "+10.0%" in texto


def test_un_trimestre_ya_publicado_se_calcula_sin_mirarse_a_si_mismo():
    # Backtest de 2026-T2: último anterior = 2026-T1, no 2026-T3 ni el propio 2026-T2.
    nivel = calcular_nivel(HISTORICO, T("2026-T2"))
    alterado = {**HISTORICO, T("2026-T2"): 9_999_999, T("2026-T3"): 1}

    assert nivel.total == 1_155_000  # 1.100.000 × (1.050.000 / 1.000.000)
    assert nivel.ultimo == T("2026-T1")
    assert calcular_nivel(alterado, T("2026-T2")).total == nivel.total


def test_el_objetivo_puede_estar_hasta_cuatro_trimestres_por_delante():
    nivel = calcular_nivel(HISTORICO, T("2027-T3"))

    assert nivel.base == T("2026-T3") and nivel.base == nivel.ultimo


def test_sin_trimestres_anteriores_falla():
    with pytest.raises(NivelError, match="ningún trimestre publicado"):
        calcular_nivel({T("2026-T4"): 1}, T("2026-T4"))


def test_mas_de_cuatro_trimestres_por_delante_falla():
    with pytest.raises(NivelError, match="más de 4 trimestres"):
        calcular_nivel(HISTORICO, T("2027-T4"))


def test_falta_el_mismo_trimestre_del_anio_anterior():
    sin_base = {k: v for k, v in HISTORICO.items() if k != T("2025-T4")}

    with pytest.raises(NivelError, match="2025-T4") as error:
        calcular_nivel(sin_base, T("2026-T4"))
    assert "--total-esperado" in str(error.value)


def test_falta_el_trimestre_de_referencia_del_crecimiento():
    sin_referencia = {k: v for k, v in HISTORICO.items() if k != T("2025-T3")}

    with pytest.raises(NivelError, match="2025-T3"):
        calcular_nivel(sin_referencia, T("2026-T4"))


def test_totales_no_positivos_se_rechazan():
    with pytest.raises(NivelError, match="positivos"):
        calcular_nivel({**HISTORICO, T("2025-T3"): 0}, T("2026-T4"))


def test_desviacion_relativa():
    assert desviacion_relativa(1_320_000, 1_200_000) == pytest.approx(0.10)
    assert desviacion_relativa(900, 1_000) == pytest.approx(-0.10)
    with pytest.raises(NivelError):
        desviacion_relativa(10, 0)
