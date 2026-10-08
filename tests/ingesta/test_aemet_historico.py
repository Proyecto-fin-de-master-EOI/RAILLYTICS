"""Tests del histórico de meteorología de AEMET.

Sin red: los tramos se siembran en la caché, que es justamente la que evita repetir peticiones a
una API gratuita y con límite. Lo delicado es el parseo de los números, que AEMET sirve como texto
con coma decimal y con marcas propias.
"""
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import requests

from raillytics.ingesta.aemet_historico import (
    _espera_tras_limite,
    _numero,
    _tramo,
    construir,
    ventanas,
)
from raillytics.ingesta.sources import load_sources

REGISTRO = Path(__file__).resolve().parents[2] / "config" / "data_sources.yml"


# --- los números de AEMET ---


@pytest.mark.parametrize(
    ("valor", "esperado"),
    [
        ("27,4", 27.4),      # coma decimal: con float() directo daría ValueError
        ("-1,5", -1.5),
        ("0,0", 0.0),
        ("Ip", 0.0),         # precipitación inapreciable: es un cero medido, no un dato que falta
        ("", None),
        ("  ", None),
        (None, None),
        ("Acum", None),      # marca de serie especial: mejor sin dato que un número inventado
    ],
)
def test_numero_de_aemet(valor, esperado):
    assert _numero(valor) == esperado


# --- troceado en tramos de 15 días ---


def test_el_rango_se_trocea_en_tramos_que_no_se_solapan():
    tramos = ventanas(date(2026, 1, 1), date(2026, 2, 14))

    assert tramos == [
        (date(2026, 1, 1), date(2026, 1, 15)),
        (date(2026, 1, 16), date(2026, 1, 30)),
        (date(2026, 1, 31), date(2026, 2, 14)),
    ]


def test_el_ultimo_tramo_se_recorta_al_final_del_rango():
    tramos = ventanas(date(2026, 1, 1), date(2026, 1, 18))

    assert tramos == [(date(2026, 1, 1), date(2026, 1, 15)), (date(2026, 1, 16), date(2026, 1, 18))]


def test_un_solo_dia_es_un_tramo():
    assert ventanas(date(2026, 1, 7), date(2026, 1, 7)) == [(date(2026, 1, 7), date(2026, 1, 7))]


# --- construcción de la tabla desde la caché, sin tocar la red ---


def _sembrar(cache: Path, fuente: str, ini: date, fin: date, registros: list[dict]) -> None:
    d = cache / fuente
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{ini.isoformat()}_{fin.isoformat()}.json").write_text(
        json.dumps(registros, ensure_ascii=False), encoding="utf-8"
    )


def test_construye_una_fila_por_dia_y_ciudad_desde_la_cache(tmp_path):
    ini, fin = date(2026, 9, 1), date(2026, 9, 2)
    _sembrar(tmp_path, "aemet_madrid_barajas", ini, fin, [
        {"fecha": "2026-09-01", "indicativo": "3195", "tmed": "27,0", "prec": "0,0"},
        {"fecha": "2026-09-02", "indicativo": "3195", "tmed": "27,4", "prec": "Ip"},
    ])
    _sembrar(tmp_path, "aemet_barcelona_prat", ini, fin, [
        {"fecha": "2026-09-01", "indicativo": "0076", "tmed": "24,1", "prec": "3,4"},
        {"fecha": "2026-09-02", "indicativo": "0076", "tmed": "", "prec": ""},
    ])

    df = construir(REGISTRO, ini, fin, tmp_path)

    assert list(df.columns) == ["fecha", "ciudad", "temperatura_media", "precipitacion_mm"]
    assert len(df) == 4
    assert sorted(df["ciudad"].unique()) == ["BCN", "MAD"]
    mad = df[(df["ciudad"] == "MAD") & (df["fecha"] == date(2026, 9, 2))].iloc[0]
    assert (mad["temperatura_media"], mad["precipitacion_mm"]) == (27.4, 0.0)
    # El día sin medida queda sin dato: el contrato permite nulos en estas dos columnas.
    bcn = df[(df["ciudad"] == "BCN") & (df["fecha"] == date(2026, 9, 2))].iloc[0]
    assert pd.isna(bcn["temperatura_media"]) and pd.isna(bcn["precipitacion_mm"])


def test_no_se_repite_ningun_par_fecha_ciudad(tmp_path):
    # El contrato de la predicción exige clave única (fecha, ciudad).
    ini, fin = date(2026, 9, 1), date(2026, 9, 1)
    for fuente in ("aemet_madrid_barajas", "aemet_barcelona_prat"):
        _sembrar(tmp_path, fuente, ini, fin, [
            {"fecha": "2026-09-01", "tmed": "20,0", "prec": "0,0"},
            {"fecha": "2026-09-01", "tmed": "21,0", "prec": "1,0"},  # repetido en el mismo tramo
        ])

    df = construir(REGISTRO, ini, fin, tmp_path)

    assert not df.duplicated(["fecha", "ciudad"]).any()
    assert len(df) == 2


def test_un_tramo_que_falla_no_tira_la_carga(monkeypatch, tmp_path):
    # Solo se siembra Madrid; el de Barcelona no está en la caché, así que iría a AEMET. Se fuerza
    # el fallo para no tocar la red: debe quedar omitido y la carga seguir con lo que sí tiene.
    def no_disponible(*_args, **_kwargs):
        raise RuntimeError("AEMET no responde")

    monkeypatch.setattr("raillytics.ingesta.aemet_historico.aemet_ventana", no_disponible)
    ini, fin = date(2026, 9, 1), date(2026, 9, 1)
    _sembrar(tmp_path, "aemet_madrid_barajas", ini, fin,
             [{"fecha": "2026-09-01", "tmed": "20,0", "prec": "0,0"}])

    df = construir(REGISTRO, ini, fin, tmp_path)

    assert sorted(df["ciudad"].unique()) == ["MAD"]


# --- limite de peticiones de AEMET ---


def _error_429(retry_after: str | None = None) -> requests.HTTPError:
    respuesta = requests.Response()
    respuesta.status_code = 429
    if retry_after is not None:
        respuesta.headers["Retry-After"] = retry_after
    return requests.HTTPError("429 Too Many Requests", response=respuesta)


def test_un_429_se_espera_y_se_reintenta_en_vez_de_perder_el_tramo(monkeypatch, tmp_path):
    # Un 429 NO significa que el tramo no exista: tratarlo como «sin datos» deja huecos en la serie
    # que parecen del origen y son nuestros.
    intentos = []
    esperas = []

    def falla_una_vez(fuente, ini, fin):
        intentos.append((ini, fin))
        if len(intentos) == 1:
            raise _error_429()
        return SimpleNamespace(content=b'[{"fecha": "2026-09-01", "tmed": "20,0", "prec": "0,0"}]')

    monkeypatch.setattr("raillytics.ingesta.aemet_historico.aemet_ventana", falla_una_vez)
    monkeypatch.setattr("raillytics.ingesta.aemet_historico.time.sleep", lambda s: esperas.append(s))

    fuente = next(f for f in load_sources(REGISTRO) if f.id == "aemet_madrid_barajas")
    registros = _tramo(fuente, date(2026, 9, 1), date(2026, 9, 1), tmp_path)

    assert len(intentos) == 2            # reintentó
    assert esperas[0] > 0                # y esperó antes
    assert registros[0]["tmed"] == "20,0"


def test_se_respeta_el_retry_after_que_manda_aemet():
    respuesta = _error_429("17").response

    assert _espera_tras_limite(respuesta, 1) == 17


def test_sin_retry_after_la_espera_crece_con_los_intentos():
    respuesta = _error_429().response

    assert _espera_tras_limite(respuesta, 1) < _espera_tras_limite(respuesta, 2)


def test_un_error_que_no_es_429_se_propaga(monkeypatch, tmp_path):
    # Un 404 sí significa que no hay datos de ese tramo: no tiene sentido reintentarlo.
    respuesta = requests.Response()
    respuesta.status_code = 404

    def falla(*_args, **_kwargs):
        raise requests.HTTPError("404", response=respuesta)

    monkeypatch.setattr("raillytics.ingesta.aemet_historico.aemet_ventana", falla)
    fuente = next(f for f in load_sources(REGISTRO) if f.id == "aemet_madrid_barajas")

    with pytest.raises(requests.HTTPError):
        _tramo(fuente, date(2026, 9, 1), date(2026, 9, 1), tmp_path)
