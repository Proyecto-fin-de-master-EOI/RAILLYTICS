"""La respuesta de Tempus3 se convierte en la tabla mensual, y lo que no encaja falla en alto."""
from __future__ import annotations

import json
from datetime import date

import pytest

from raillytics.ingesta.ine_turismo import parsear


def _serie(nombre="Turistas. Madrid. Barcelona. Dato base.", datos=None):
    return {
        "COD": "TMOV123",
        "Nombre": nombre,
        "FK_Unidad": 256,
        "FK_Escala": 1,
        "Data": datos if datos is not None else [
            {"Fecha": 0, "FK_TipoDato": 1, "FK_Periodo": 1, "Anyo": 2026, "Valor": 59702.0, "Secreto": False},
            {"Fecha": 0, "FK_TipoDato": 2, "FK_Periodo": 2, "Anyo": 2026, "Valor": 69774.0, "Secreto": False},
        ],
    }


def _bytes(*series) -> bytes:
    return json.dumps(list(series), ensure_ascii=False).encode("utf-8")


def test_cada_mes_se_representa_por_su_primer_dia_y_se_etiqueta_el_tipo_de_dato():
    df = parsear(_bytes(_serie()))

    assert list(df["fecha"]) == [date(2026, 1, 1), date(2026, 2, 1)]
    assert list(df["origen"]) == ["Madrid", "Madrid"]
    assert list(df["destino"]) == ["Barcelona", "Barcelona"]
    assert list(df["turistas"]) == [59702.0, 69774.0]
    # El INE marca los meses recientes como provisionales: se guarda, no se descarta.
    assert list(df["tipo_dato"]) == ["definitivo", "provisional"]


def test_un_valor_con_secreto_estadistico_no_entra_como_cero():
    oculto = [
        {"FK_TipoDato": 1, "FK_Periodo": 1, "Anyo": 2026, "Valor": 0.0, "Secreto": True},
        {"FK_TipoDato": 1, "FK_Periodo": 2, "Anyo": 2026, "Valor": 69774.0, "Secreto": False},
    ]

    df = parsear(_bytes(_serie(datos=oculto)))

    assert list(df["fecha"]) == [date(2026, 2, 1)]


def test_una_serie_que_no_es_mensual_falla_en_vez_de_dar_un_perfil_mal():
    # Tempus3 usa FK_Periodo para meses, trimestres y semestres con el mismo campo. Un 13 es un
    # trimestre, y tomarlo por un mes daría un perfil silenciosamente equivocado.
    trimestral = [{"FK_TipoDato": 1, "FK_Periodo": 13, "Anyo": 2026, "Valor": 1.0, "Secreto": False}]

    with pytest.raises(ValueError, match="no es un mes"):
        parsear(_bytes(_serie(datos=trimestral)))


def test_si_el_ine_cambia_el_nombre_de_las_series_no_se_etiquetan_mal_las_filas():
    with pytest.raises(ValueError, match="no tiene la forma"):
        parsear(_bytes(_serie(nombre="Viajeros por provincia")))


def test_el_mismo_mes_dos_veces_para_un_destino_es_un_error():
    repetido = [
        {"FK_TipoDato": 1, "FK_Periodo": 3, "Anyo": 2026, "Valor": 1.0, "Secreto": False},
        {"FK_TipoDato": 1, "FK_Periodo": 3, "Anyo": 2026, "Valor": 2.0, "Secreto": False},
    ]

    with pytest.raises(ValueError, match="meses repetidos"):
        parsear(_bytes(_serie(datos=repetido)))


def test_una_respuesta_vacia_o_que_no_es_una_lista_de_series_falla():
    with pytest.raises(ValueError, match="no es una lista de series"):
        parsear(b"[]")
    with pytest.raises(ValueError, match="no es una lista de series"):
        parsear(b'{"error": "parametros incorrectos"}')


def test_varias_series_en_una_respuesta_se_apilan_con_su_destino():
    df = parsear(_bytes(_serie(), _serie(nombre="Turistas. Madrid. Valencia/València. Dato base.")))

    assert sorted(df["destino"].unique()) == ["Barcelona", "Valencia/València"]
    assert len(df) == 4
