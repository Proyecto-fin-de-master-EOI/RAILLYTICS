"""El reparto de la cuota medida de CNMC entre las marcas comerciales del generador."""
from __future__ import annotations

import pytest

from raillytics.procesamiento.cuotas_operador import CNMC_POR_MARCA, por_marca
from raillytics.procesamiento.silver_sample import OPERADORES

# Lo que publica CNMC del corredor: una fila por empresa, y Avlo va dentro de Renfe Viajeros.
MEDIDO = {"RENFE": 0.603, "IRYO": 0.237, "OUIGO": 0.160}


def test_una_marca_que_cnmc_publica_sola_se_lleva_su_cuota_entera():
    propuesto = por_marca(MEDIDO, {"IRYO": 0.25, "OUIGO": 0.20})

    assert propuesto == {"IRYO": 0.237, "OUIGO": 0.160}


def test_las_marcas_de_una_misma_empresa_reparten_su_cuota_en_la_proporcion_actual():
    # CNMC agrega Renfe y Avlo: el 60,3 % es dato, el 72,7/27,3 entre ellas es el supuesto que ya
    # traía el generador y se conserva.
    propuesto = por_marca(MEDIDO, {"RENFE": 0.40, "AVLO": 0.15})

    assert propuesto["RENFE"] == pytest.approx(0.439, abs=0.001)
    assert propuesto["AVLO"] == pytest.approx(0.164, abs=0.001)
    assert propuesto["RENFE"] + propuesto["AVLO"] == pytest.approx(0.603, abs=0.001)


def test_lo_que_cnmc_no_publica_no_se_inventa():
    # Si apareciera una marca de una empresa que CNMC no desglosa, queda fuera en vez de recibir una
    # cuota repartida a ojo.
    propuesto = por_marca(MEDIDO, {"IRYO": 0.25, "DESCONOCIDO": 0.10})

    assert "DESCONOCIDO" not in propuesto


def test_si_las_proporciones_actuales_son_cero_la_cuota_se_parte_a_partes_iguales():
    # Caso límite: sin proporción de partida no hay supuesto que conservar, y repartir a partes
    # iguales es explícito en vez de dividir por cero.
    propuesto = por_marca(MEDIDO, {"RENFE": 0.0, "AVLO": 0.0})

    assert propuesto["RENFE"] == propuesto["AVLO"] == pytest.approx(0.3015, abs=0.001)


def test_las_cuotas_del_generador_son_las_que_salen_de_lo_medido():
    # El test que cierra el círculo: los valores escritos a mano en OPERADORES tienen que ser los
    # que produce aplicar el reparto a la cuota de CNMC. Si alguien los toca sin recalcular, falla.
    actuales = {op.id: op.cuota for op in OPERADORES}

    propuesto = por_marca(MEDIDO, actuales)

    for operador, cuota in propuesto.items():
        assert actuales[operador] == pytest.approx(cuota, abs=0.001), operador


def test_solo_las_marcas_que_cnmc_agrega_necesitan_mapeo():
    # Iryo y OUIGO usan el mismo id en los dos sitios. Una entrada de más aquí significaría que
    # estamos agregando algo que CNMC sí separa.
    assert CNMC_POR_MARCA == {"AVLO": "RENFE"}
