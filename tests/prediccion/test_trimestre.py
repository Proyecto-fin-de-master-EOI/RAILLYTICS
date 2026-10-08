from datetime import date

import pytest

from raillytics.prediccion.trimestre import Trimestre


def test_parse_y_str_son_inversos():
    assert str(Trimestre.parse("2026-T4")) == "2026-T4"
    assert Trimestre.parse(" 2026-T1 ") == Trimestre(2026, 1)


@pytest.mark.parametrize("texto", ["2026-T5", "2026-T0", "2026T4", "26-T4", "2026-t4", "T4-2026", ""])
def test_parse_rechaza_formatos_invalidos(texto):
    with pytest.raises(ValueError, match="AAAA-Tn"):
        Trimestre.parse(texto)


def test_el_constructor_rechaza_numeros_fuera_de_rango():
    with pytest.raises(ValueError, match="fuera de rango"):
        Trimestre(2026, 5)


@pytest.mark.parametrize(
    "dia, esperado",
    [
        (date(2026, 1, 1), Trimestre(2026, 1)), (date(2026, 3, 31), Trimestre(2026, 1)),
        (date(2026, 4, 1), Trimestre(2026, 2)), (date(2026, 10, 1), Trimestre(2026, 4)),
        (date(2026, 12, 31), Trimestre(2026, 4)), (date(2028, 2, 29), Trimestre(2028, 1)),
    ],
)
def test_de_fecha_devuelve_el_trimestre_que_contiene_el_dia(dia, esperado):
    assert Trimestre.de_fecha(dia) == esperado
    assert esperado.inicio <= dia <= esperado.fin


def test_aritmetica_cruza_el_cambio_de_anio():
    assert Trimestre(2026, 4).mas(1) == Trimestre(2027, 1)
    assert Trimestre(2026, 1).menos(1) == Trimestre(2025, 4)
    assert Trimestre(2026, 1).menos(4) == Trimestre(2025, 1)
    assert Trimestre(2026, 3).mas(6) == Trimestre(2028, 1)


def test_distancia_y_orden():
    assert Trimestre(2026, 4).distancia(Trimestre(2026, 2)) == 2
    assert Trimestre(2026, 1).distancia(Trimestre(2025, 4)) == 1
    assert sorted([Trimestre(2026, 1), Trimestre(2025, 4)]) == [Trimestre(2025, 4), Trimestre(2026, 1)]


@pytest.mark.parametrize(
    "anio, dias_por_trimestre",
    [(2026, [90, 91, 92, 92]), (2028, [91, 91, 92, 92])],  # 2028 es bisiesto
)
def test_numero_de_dias_de_cada_trimestre(anio, dias_por_trimestre):
    assert [len(Trimestre(anio, n).dias()) for n in (1, 2, 3, 4)] == dias_por_trimestre


def test_el_t1_de_un_anio_bisiesto_incluye_el_29_de_febrero():
    dias = Trimestre(2028, 1).dias()

    assert date(2028, 2, 29) in dias
    assert dias[0] == date(2028, 1, 1) and dias[-1] == date(2028, 3, 31)


def test_limites_del_trimestre_que_cierra_el_anio():
    t = Trimestre(2026, 4)

    assert (t.inicio, t.fin) == (date(2026, 10, 1), date(2026, 12, 31))
    assert t.dias()[0] == t.inicio and t.dias()[-1] == t.fin
