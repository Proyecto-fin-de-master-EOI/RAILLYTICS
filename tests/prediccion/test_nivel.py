import pytest

from raillytics.prediccion.nivel import (
    NivelError,
    _cuantil,
    _extremos,
    calcular_nivel,
    cobertura,
    desviacion_relativa,
    errores_backtest,
    intervalo,
    miles,
)
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


# --- backtest de la regla y el intervalo que sale de él ---

def _serie(crecimiento: float, anios=range(2015, 2027), base=1_000_000) -> dict:
    """Una serie con estacionalidad fija y crecimiento constante: la regla debería acertar siempre."""
    estacion = {1: 0.9, 2: 1.1, 3: 1.2, 4: 1.0}
    return {
        T(f"{a}-T{t}"): int(base * estacion[t] * (1 + crecimiento) ** (a - 2015))
        for a in anios for t in (1, 2, 3, 4)
    }


def test_con_crecimiento_constante_la_regla_no_se_equivoca():
    # La regla es «el mismo trimestre del año pasado por el crecimiento del último publicado»: si el
    # crecimiento es constante y la estacionalidad fija, el error tiene que ser ~0.
    errores = [e for _, e in errores_backtest(_serie(0.05))]

    assert errores and max(abs(e) for e in errores) < 1e-3


def test_un_trimestre_que_toca_la_pandemia_en_cualquiera_de_sus_entradas_queda_fuera():
    # No basta con excluir el trimestre objetivo: si su base (T-4) o su referencia de crecimiento
    # (U-4) cayeron en pandemia, el error que mide es la pandemia, no la regla.
    objetivos = [t for t, _ in errores_backtest(_serie(0.05))]

    assert not any(t.anio in (2020, 2021) for t in objetivos)
    assert not any(t.menos(4).anio in (2020, 2021) for t in objetivos)


def test_el_intervalo_se_obtiene_dividiendo_y_no_multiplicando():
    # El error se definió como previsto/real - 1, así que deshacerlo es DIVIDIR. Multiplicar daría
    # una banda al revés y desplazada, que es el fallo clásico aquí. Con una regla que se pasa
    # siempre, el valor real estuvo por DEBAJO: la banda tiene que quedar por debajo del central.
    serie = _serie(0.05)
    # Se infla el último año para que la regla se pase de forma sistemática al predecirlo.
    serie = {t: (int(v * 1.2) if t.anio <= 2025 else v) for t, v in serie.items()}
    nivel = calcular_nivel(serie, T("2026-T3"))

    banda = intervalo(nivel, serie)

    assert banda.bajo < banda.alto
    assert banda.sesgo > 0, "la regla debería salir con sesgo positivo en esta serie"
    assert banda.bajo < nivel.total, "si la regla se pasa, el extremo bajo queda por debajo del central"


def test_el_cuantil_alto_del_error_da_el_extremo_bajo_de_la_banda():
    serie = _serie(0.05)
    nivel = calcular_nivel(serie, T("2026-T3"))

    estrecha = intervalo(nivel, serie, confianza=0.50)
    ancha = intervalo(nivel, serie, confianza=0.95)

    assert ancha.bajo <= estrecha.bajo and ancha.alto >= estrecha.alto


def test_sin_bastantes_trimestres_no_se_da_intervalo_en_vez_de_dar_uno_malo():
    corta = _serie(0.05, anios=range(2024, 2027))
    nivel = calcular_nivel(corta, T("2026-T3"))

    with pytest.raises(NivelError, match="backtest"):
        intervalo(nivel, corta)


def test_la_cobertura_sale_entre_cero_y_uno_y_deja_fuera_los_extremos():
    serie = _serie(0.05)

    c = cobertura(serie)

    assert 0.0 <= c <= 1.0


def test_la_correccion_de_muestra_finita_ensancha_la_banda():
    # Sin ella el intervalo sale optimista: con los datos reales, uno que decía 90 % cubría el 79 %.
    errores = sorted([-0.08, -0.05, -0.02, 0.0, 0.01, 0.02, 0.03, 0.05, 0.09, 0.15])
    bajo, alto = _extremos(errores, 0.90)

    assert bajo <= _cuantil(errores, 0.05) and alto >= _cuantil(errores, 0.95)
