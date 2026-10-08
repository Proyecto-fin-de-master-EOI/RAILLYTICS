import json

import pytest
import requests
from requests_mock import ANY

from raillytics.ingesta.downloaders import obtener
from raillytics.ingesta.sources import DataSource

AEMET_URL = (
    "https://aemet.example.invalid/datos/fechaini/{fecha_ini}T00:00:00UTC"
    "/fechafin/{fecha_fin}T23:59:59UTC/estacion/3195"
)
NAP_URL = "https://nap.example.invalid/api/v2/conjunto-dato/897/historico"


def _fuente(**kwargs) -> DataSource:
    base = dict(id="fuente", name="Fuente de prueba", url="https://ejemplo.invalid/d", format="json")
    base.update(kwargs)
    return DataSource(**base)


# --- http: el descargador por defecto ---


def test_http_descarga_sin_credencial(requests_mock):
    requests_mock.get("https://ejemplo.invalid/d", content=b"{}", headers={"Content-Type": "application/json"})

    resultado = obtener(_fuente())

    assert resultado.content == b"{}"
    assert "ApiKey" not in requests_mock.last_request.headers


def test_http_manda_la_credencial_en_la_cabecera_declarada(monkeypatch, requests_mock):
    monkeypatch.setenv("MI_CLAVE", "secreto")
    requests_mock.get("https://ejemplo.invalid/d", content=b"{}")

    obtener(_fuente(auth={"env": "MI_CLAVE", "header": "ApiKey"}))

    assert requests_mock.last_request.headers["ApiKey"] == "secreto"


def test_falta_la_variable_de_entorno_y_el_error_la_nombra(monkeypatch):
    monkeypatch.delenv("NO_DEFINIDA", raising=False)

    with pytest.raises(RuntimeError, match="NO_DEFINIDA"):
        obtener(_fuente(auth={"env": "NO_DEFINIDA"}))


# --- aemet: plantilla de fechas y descarga en dos pasos ---


def test_aemet_rellena_las_fechas_y_sigue_el_enlace_temporal(monkeypatch, requests_mock):
    monkeypatch.setenv("AEMET", "clave")
    requests_mock.get(
        ANY, json={"estado": 200, "datos": "https://aemet.example.invalid/sh/abc"}
    )
    requests_mock.get("https://aemet.example.invalid/sh/abc", content=b'[{"fecha":"2026-09-01"}]')

    resultado = obtener(_fuente(url=AEMET_URL, downloader="aemet", auth={"env": "AEMET", "header": "api_key"}))

    assert resultado.content == b'[{"fecha":"2026-09-01"}]'
    # La plantilla se resolvió: no queda ningún marcador sin sustituir en la URL pedida.
    pedidas = [r.url for r in requests_mock.request_history]
    assert not any("{fecha" in u for u in pedidas)
    # Y el fichero conserva un nombre con el rango, para que se distingan en el staging.
    assert "filename=" in resultado.headers["Content-Disposition"]


def test_aemet_reintenta_cuando_el_enlace_temporal_da_500(monkeypatch, requests_mock):
    monkeypatch.setenv("AEMET", "clave")
    monkeypatch.setattr("raillytics.ingesta.downloaders.AEMET_ESPERA_SEGUNDOS", 0)
    requests_mock.get(
        ANY, json={"estado": 200, "datos": "https://aemet.example.invalid/sh/abc"}
    )
    requests_mock.get(
        "https://aemet.example.invalid/sh/abc",
        [{"status_code": 500}, {"content": b"[]", "status_code": 200}],
    )

    resultado = obtener(_fuente(url=AEMET_URL, downloader="aemet", auth={"env": "AEMET"}))

    assert resultado.content == b"[]"


def test_aemet_propaga_el_error_si_el_enlace_nunca_responde(monkeypatch, requests_mock):
    monkeypatch.setenv("AEMET", "clave")
    monkeypatch.setattr("raillytics.ingesta.downloaders.AEMET_ESPERA_SEGUNDOS", 0)
    requests_mock.get(
        ANY, json={"estado": 200, "datos": "https://aemet.example.invalid/sh/abc"}
    )
    requests_mock.get("https://aemet.example.invalid/sh/abc", status_code=500)

    with pytest.raises(requests.HTTPError):
        obtener(_fuente(url=AEMET_URL, downloader="aemet", auth={"env": "AEMET"}))


def test_aemet_falla_con_la_descripcion_si_la_api_rechaza(monkeypatch, requests_mock):
    monkeypatch.setenv("AEMET", "clave")
    requests_mock.get(ANY, json={"estado": 404, "descripcion": "No hay datos"})

    with pytest.raises(RuntimeError, match="No hay datos"):
        obtener(_fuente(url=AEMET_URL, downloader="aemet", auth={"env": "AEMET"}))


# --- nap: listar, elegir el más reciente y seguir el enlace firmado ---


def test_nap_elige_el_snapshot_mas_reciente_y_corrige_la_ruta(monkeypatch, requests_mock):
    monkeypatch.setenv("NAP", "clave")
    requests_mock.get(
        NAP_URL,
        json={"data": [
            {"id": 1, "fecha": "2026-10-01T00:00:00", "nombreArchivo": "viejo"},
            {"id": 2, "fecha": "2026-10-03T00:00:00", "nombreArchivo": "nuevo"},
        ]},
    )
    # La ruta correcta es /descarga/historico; el `enlaceDescarga` del listado viene invertido y da 404.
    requests_mock.get(
        "https://nap.example.invalid/api/v2/fichero/2/descarga/historico",
        json={"data": {"enlaceDescarga": "https://s3.example.invalid/firmado", "nombreFichero": "nuevo.zip"}},
    )
    requests_mock.get("https://s3.example.invalid/firmado", content=b"PK\x03\x04zip")

    resultado = obtener(_fuente(url=NAP_URL, format="zip", downloader="nap", auth={"env": "NAP", "header": "ApiKey"}))

    assert resultado.content == b"PK\x03\x04zip"
    assert 'filename="nuevo.zip"' in resultado.headers["Content-Disposition"]
    # La URL firmada ya lleva la autorización en la firma: no se le manda la credencial.
    assert "ApiKey" not in requests_mock.request_history[-1].headers


def test_nap_falla_claro_si_el_conjunto_no_tiene_snapshots(monkeypatch, requests_mock):
    monkeypatch.setenv("NAP", "clave")
    requests_mock.get(NAP_URL, json={"data": []})

    with pytest.raises(RuntimeError, match="ningún snapshot"):
        obtener(_fuente(url=NAP_URL, format="zip", downloader="nap", auth={"env": "NAP"}))


def test_nap_falla_claro_si_no_devuelve_enlace(monkeypatch, requests_mock):
    monkeypatch.setenv("NAP", "clave")
    requests_mock.get(NAP_URL, json={"data": [{"id": 7, "fecha": "2026-10-03T00:00:00"}]})
    requests_mock.get("https://nap.example.invalid/api/v2/fichero/7/descarga/historico", json={"data": {}})

    with pytest.raises(RuntimeError, match="enlace de descarga"):
        obtener(_fuente(url=NAP_URL, format="zip", downloader="nap", auth={"env": "NAP"}))
