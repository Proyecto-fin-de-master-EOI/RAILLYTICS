import random

import pytest

from raillytics.prediccion.normalizar import INDICE_MAX, INDICE_MIN, normalizar_indices, repartir


def test_los_limites_del_indice_coinciden_con_el_spec():
    assert (INDICE_MIN, INDICE_MAX) == (0.2, 3.0)


def test_repartir_es_proporcional():
    assert repartir([2.0, 1.0], 3) == [2, 1]
    assert repartir([1.0, 3.0], 400) == [100, 300]


def test_el_resto_va_a_los_mayores_decimales_y_los_empates_al_dia_mas_temprano():
    assert repartir([1.0, 1.0, 1.0], 100) == [34, 33, 33]
    assert repartir([1.0, 1.0, 1.0], 101) == [34, 34, 33]


def test_la_suma_es_exacta_en_muchos_casos_aleatorios():
    azar = random.Random(1)
    for _ in range(500):
        n, total = azar.randint(1, 95), azar.randint(0, 5_000_000)
        indices = [round(azar.uniform(0.2, 3.0), azar.choice([1, 2, 3, 6])) for _ in range(n)]

        reparto = repartir(indices, total)

        assert sum(reparto) == total and all(v >= 0 for v in reparto)


def test_repartir_es_determinista():
    indices = [0.1, 0.2, 0.3, 0.4, 1.3]

    assert repartir(indices, 7) == repartir(indices, 7)


def test_un_total_menor_que_los_dias_deja_dias_a_cero_sin_romper_la_suma():
    reparto = repartir([1.0] * 10, 3)

    assert sum(reparto) == 3 and sorted(reparto) == [0] * 7 + [1] * 3


def test_total_cero_reparte_ceros():
    assert repartir([1.0, 2.0], 0) == [0, 0]


def test_algun_indice_cero_es_valido_pero_todos_cero_no():
    assert repartir([0.0, 1.0], 5) == [0, 5]
    with pytest.raises(ValueError, match="suma de los índices"):
        repartir([0.0, 0.0], 10)


@pytest.mark.parametrize("indices, total", [([], 10), ([1.0, -0.5], 10), ([1.0], -1)])
def test_repartir_rechaza_entradas_invalidas(indices, total):
    with pytest.raises(ValueError):
        repartir(indices, total)


def test_normalizar_indices_deja_la_media_en_uno_y_conserva_las_proporciones():
    normalizados = normalizar_indices([2.0, 4.0, 6.0])

    assert sum(normalizados) / 3 == pytest.approx(1.0)
    assert normalizados[1] / normalizados[0] == pytest.approx(2.0)


def test_normalizar_indices_rechaza_suma_no_positiva():
    with pytest.raises(ValueError):
        normalizar_indices([0.0, 0.0])
    with pytest.raises(ValueError):
        normalizar_indices([])
