import io
import re
import zipfile
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
import requests

from raillytics.ingesta.download import download
from raillytics.ingesta.sources import DataSource

SOURCE = DataSource(
    id="crtm",
    name="CRTM test",
    url="https://crtm.example.invalid/data",
    format="csv",
)

_TIMESTAMP_PREFIX_RE = re.compile(r"^\d{8}_\d{6}_")
CSV = b"estacion,viajeros\nAtocha,100\n"


def _zip(*miembros: tuple[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for nombre, contenido in miembros:
            zf.writestr(nombre, contenido)
    return buf.getvalue()


def test_download_writes_file_using_content_disposition_name(tmp_path, requests_mock):
    requests_mock.get(
        SOURCE.url,
        content=CSV,
        headers={"Content-Disposition": 'attachment; filename="crtm_export.csv"', "Content-Type": "text/csv"},
    )

    result = download(SOURCE, tmp_path)

    # El nombre de Content-Disposition se conserva para trazabilidad, pero
    # siempre va prefijado con un timestamp para que cada descarga produzca
    # un nombre distinto (ver test_download_produces_unique_filenames_across_calls).
    assert result.aceptada and result.motivo_rechazo is None
    assert result.path.name.endswith("_crtm_export.csv")
    assert _TIMESTAMP_PREFIX_RE.match(result.path.name)
    assert result.path.parent == tmp_path / "crtm"
    assert result.path.read_bytes() == CSV
    assert result.bytes == len(CSV)
    assert [g.resultado for g in result.gates] == ["ok", "ok", "ok"]


def test_download_falls_back_to_generated_name_without_content_disposition(tmp_path, requests_mock):
    requests_mock.get(SOURCE.url, content=CSV)

    result = download(SOURCE, tmp_path)

    assert result.path.parent == tmp_path / "crtm"
    assert _TIMESTAMP_PREFIX_RE.match(result.path.name)
    assert result.path.name.endswith("_crtm.csv")


def test_download_raises_on_http_error(tmp_path, requests_mock):
    requests_mock.get(SOURCE.url, status_code=500)

    with pytest.raises(requests.HTTPError):
        download(SOURCE, tmp_path)


def test_download_sanitizes_path_traversal_in_content_disposition(tmp_path, requests_mock):
    """Verify that malicious Content-Disposition with path traversal falls back to timestamp name."""
    requests_mock.get(
        SOURCE.url,
        content=CSV,
        headers={"Content-Disposition": 'attachment; filename="../../../etc/passwd"'},
    )

    result = download(SOURCE, tmp_path)

    # Result should still be under source.id directory, not escaped
    assert result.path.parent == tmp_path / "crtm"
    # Should have used fallback timestamp name, not the malicious filename
    assert _TIMESTAMP_PREFIX_RE.match(result.path.name)
    assert result.path.name.endswith("_crtm.csv")
    assert ".." not in str(result.path)


def test_download_strips_trailing_parameters_from_content_disposition_filename(tmp_path, requests_mock):
    """Regression test: a trailing '; size=...' parameter must not leak into the filename."""
    requests_mock.get(
        SOURCE.url,
        content=CSV,
        headers={
            "Content-Disposition": 'attachment; filename="google_transit_M5.csv"; size=6042'
        },
    )

    result = download(SOURCE, tmp_path)

    assert result.path.name.endswith("_google_transit_M5.csv")
    assert "size=6042" not in result.path.name
    assert '"' not in result.path.name


def test_download_produces_unique_filenames_across_calls(tmp_path, requests_mock):
    """Regression test for the Critical finding: repeated downloads with an identical
    Content-Disposition header (as observed against the real CRTM URL) must not collide
    on the same filename, or Spark's file-streaming source will silently skip every
    download after the first forever."""
    requests_mock.get(
        SOURCE.url,
        content=CSV,
        headers={"Content-Disposition": 'attachment; filename="google_transit_M5.csv"; size=6042'},
    )

    # Fuerza timestamps distintos entre llamadas para que el test no dependa
    # de que las dos descargas caigan en segundos de reloj distintos (la
    # resolución de staging_filename es de un segundo).
    now = datetime.now()
    with patch("raillytics.ingesta.filenames.datetime") as mock_datetime:
        mock_datetime.now.side_effect = [now, now + timedelta(seconds=1)]
        first = download(SOURCE, tmp_path)
        second = download(SOURCE, tmp_path)

    assert first.path != second.path
    assert first.path.name != second.path.name


def test_download_quarantines_a_zip_declared_as_csv(tmp_path, requests_mock):
    """Regression test: the real CRTM URL serves a GTFS zip. Declared as csv, it used to
    land in staging and L2 converted the binary bytes to Parquet without complaint."""
    gtfs = _zip(("stops.txt", b"stop_id,stop_name\n1,Atocha\n"))
    requests_mock.get(
        SOURCE.url,
        content=gtfs,
        headers={"Content-Disposition": 'attachment; filename="google_transit_M5.zip"; size=6042', "Content-Type": "application/zip"},
    )

    result = download(SOURCE, tmp_path / "bronze")

    assert not result.aceptada
    assert "formato_declarado" in result.motivo_rechazo and "parece ZIP" in result.motivo_rechazo
    # El fichero va a la cuarentena hermana del staging, con su motivo al lado, y el staging queda limpio.
    assert result.path.parent == tmp_path / "bronze_rejected" / "crtm"
    assert result.path.read_bytes() == gtfs
    assert (result.path.parent / f"{result.path.name}.rechazo.txt").read_text(encoding="utf-8").startswith("formato_declarado:")
    assert not (tmp_path / "bronze" / "crtm").exists()
    assert {g.gate: g.resultado for g in result.gates} == {
        "contenido_no_vacio": "ok", "formato_declarado": "fallo", "content_type": "fallo"}


def test_download_accepts_the_zip_when_the_source_declares_zip_and_honours_rejected_root(tmp_path, requests_mock):
    zip_source = DataSource(id="crtm", name="CRTM zip", url=SOURCE.url, format="zip")
    requests_mock.get(SOURCE.url, content=_zip(("stops.txt", b"a,b\n1,2\n")), headers={"Content-Type": "application/zip"})
    requests_mock.get("https://empty.example.invalid/", content=b"")
    empty_source = DataSource(id="vacio", name="Vacío", url="https://empty.example.invalid/", format="json")

    ok = download(zip_source, tmp_path / "bronze", rejected_root=tmp_path / "cuarentena")
    rejected = download(empty_source, tmp_path / "bronze", rejected_root=tmp_path / "cuarentena")

    assert ok.aceptada and ok.path.parent == tmp_path / "bronze" / "crtm"
    assert not rejected.aceptada and rejected.path.parent == tmp_path / "cuarentena" / "vacio"
    assert rejected.motivo_rechazo == "contenido_no_vacio: el servidor devolvió 0 bytes"


CNMC_CSV = "﻿Trimestre;Viajeros (Núm)\n2026T2;100\n2026T1;90\n".encode("utf-8")
CNMC = DataSource(
    id="cnmc", name="CNMC prueba", url="https://cnmc.example.invalid/ds.csv", format="csv",
    options={"delimiter": ";"}, checks={"min_filas": 2, "columnas": ["Trimestre", "Viajeros (Núm)"]},
)


def test_un_csv_con_las_reglas_de_la_fuente_cumplidas_se_acepta(tmp_path, requests_mock):
    requests_mock.get(CNMC.url, content=CNMC_CSV, headers={"Content-Type": "text/csv"})

    resultado = download(CNMC, tmp_path)

    assert resultado.aceptada
    assert [g.gate for g in resultado.gates][-2:] == ["cabecera_esperada", "filas_minimas"]
    assert resultado.path.parent == tmp_path / "cnmc"


def test_un_csv_cuya_cabecera_cambio_va_a_cuarentena_con_su_motivo(tmp_path, requests_mock):
    requests_mock.get(CNMC.url, content=CNMC_CSV.replace(b"Trimestre", b"Periodo"), headers={"Content-Type": "text/csv"})

    resultado = download(CNMC, tmp_path / "bronze", tmp_path / "rechazados")

    assert not resultado.aceptada and "cabecera_esperada" in resultado.motivo_rechazo
    assert not (tmp_path / "bronze" / "cnmc").exists()
    rechazado = resultado.path
    assert rechazado.parent == tmp_path / "rechazados" / "cnmc"
    assert "Periodo" in (rechazado.parent / f"{rechazado.name}.rechazo.txt").read_text(encoding="utf-8")


# --- reintentos ante fallos de fuente (KPI «tasa de recuperación» del plan) ---------------------
#
# Lo que se vigila aquí es la DISTINCIÓN, no el bucle: reintentar lo que no se arregla reintentando
# retrasa el fallo y encima lo disfraza de recuperación en el KPI.


def test_un_corte_de_red_se_reintenta_y_la_descarga_se_recupera(tmp_path, requests_mock, monkeypatch):
    monkeypatch.setattr("raillytics.ingesta.download.time.sleep", lambda _: None)
    requests_mock.get(SOURCE.url, [
        {"exc": requests.ConnectionError},
        {"exc": requests.ConnectionError},
        {"content": CSV, "headers": {"Content-Type": "text/csv"}},
    ])

    descarga = download(SOURCE, tmp_path)

    assert descarga.aceptada
    assert descarga.intentos == 3 and descarga.recuperada


def test_un_5xx_tambien_se_reintenta(tmp_path, requests_mock, monkeypatch):
    monkeypatch.setattr("raillytics.ingesta.download.time.sleep", lambda _: None)
    requests_mock.get(SOURCE.url, [
        {"status_code": 503},
        {"content": CSV, "headers": {"Content-Type": "text/csv"}},
    ])

    descarga = download(SOURCE, tmp_path)

    assert descarga.aceptada and descarga.intentos == 2


def test_un_404_no_se_reintenta_porque_no_se_arregla_solo(tmp_path, requests_mock, monkeypatch):
    monkeypatch.setattr("raillytics.ingesta.download.time.sleep", lambda _: None)
    requests_mock.get(SOURCE.url, status_code=404)

    with pytest.raises(requests.HTTPError):
        download(SOURCE, tmp_path)

    assert requests_mock.call_count == 1   # un solo intento: la URL o la credencial están mal


def test_un_fichero_rechazado_no_se_reintenta_y_no_cuenta_como_recuperado(tmp_path, requests_mock, monkeypatch):
    """Si el NAP publica un ZIP que no es un GTFS, volver a bajarlo da el mismo ZIP. Eso tiene que
    verse en rojo, no convertirse en una recuperación."""
    monkeypatch.setattr("raillytics.ingesta.download.time.sleep", lambda _: None)
    requests_mock.get(SOURCE.url, content=_zip(("x.bin", b"\x00\x01")), headers={"Content-Type": "application/zip"})

    descarga = download(SOURCE, tmp_path)

    assert not descarga.aceptada
    assert requests_mock.call_count == 1
    assert descarga.intentos == 1 and not descarga.recuperada


def test_si_falla_en_todos_los_intentos_se_propaga_el_error(tmp_path, requests_mock, monkeypatch):
    monkeypatch.setattr("raillytics.ingesta.download.time.sleep", lambda _: None)
    requests_mock.get(SOURCE.url, exc=requests.ConnectionError)

    with pytest.raises(requests.ConnectionError):
        download(SOURCE, tmp_path, reintentos=3)

    assert requests_mock.call_count == 3
