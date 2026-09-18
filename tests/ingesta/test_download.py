import re
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


def test_download_writes_file_using_content_disposition_name(tmp_path, requests_mock):
    requests_mock.get(
        SOURCE.url,
        content=b"estacion,viajeros\nAtocha,100\n",
        headers={"Content-Disposition": 'attachment; filename="crtm_export.csv"'},
    )

    result = download(SOURCE, tmp_path)

    # El nombre de Content-Disposition se conserva para trazabilidad, pero
    # siempre va prefijado con un timestamp para que cada descarga produzca
    # un nombre distinto (ver test_download_produces_unique_filenames_across_calls).
    assert result.name.endswith("_crtm_export.csv")
    assert _TIMESTAMP_PREFIX_RE.match(result.name)
    assert result.parent == tmp_path / "crtm"
    assert result.read_bytes() == b"estacion,viajeros\nAtocha,100\n"


def test_download_falls_back_to_generated_name_without_content_disposition(tmp_path, requests_mock):
    requests_mock.get(SOURCE.url, content=b"data")

    result = download(SOURCE, tmp_path)

    assert result.parent == tmp_path / "crtm"
    assert _TIMESTAMP_PREFIX_RE.match(result.name)
    assert result.name.endswith("_crtm.csv")


def test_download_raises_on_http_error(tmp_path, requests_mock):
    requests_mock.get(SOURCE.url, status_code=500)

    with pytest.raises(requests.HTTPError):
        download(SOURCE, tmp_path)


def test_download_sanitizes_path_traversal_in_content_disposition(tmp_path, requests_mock):
    """Verify that malicious Content-Disposition with path traversal falls back to timestamp name."""
    requests_mock.get(
        SOURCE.url,
        content=b"data",
        headers={"Content-Disposition": 'attachment; filename="../../../etc/passwd"'},
    )

    result = download(SOURCE, tmp_path)

    # Result should still be under source.id directory, not escaped
    assert result.parent == tmp_path / "crtm"
    # Should have used fallback timestamp name, not the malicious filename
    assert _TIMESTAMP_PREFIX_RE.match(result.name)
    assert result.name.endswith("_crtm.csv")
    assert ".." not in str(result)


def test_download_strips_trailing_parameters_from_content_disposition_filename(tmp_path, requests_mock):
    """Regression test: a trailing '; size=...' parameter must not leak into the filename."""
    requests_mock.get(
        SOURCE.url,
        content=b"data",
        headers={
            "Content-Disposition": 'attachment; filename="google_transit_M5.zip"; size=6042'
        },
    )

    result = download(SOURCE, tmp_path)

    assert result.name.endswith("_google_transit_M5.zip")
    assert "size=6042" not in result.name
    assert '"' not in result.name


def test_download_produces_unique_filenames_across_calls(tmp_path, requests_mock):
    """Regression test for the Critical finding: repeated downloads with an identical
    Content-Disposition header (as observed against the real CRTM URL) must not collide
    on the same filename, or Spark's file-streaming source will silently skip every
    download after the first forever."""
    requests_mock.get(
        SOURCE.url,
        content=b"data",
        headers={"Content-Disposition": 'attachment; filename="google_transit_M5.zip"; size=6042'},
    )

    # Fuerza timestamps distintos entre llamadas para que el test no dependa
    # de que las dos descargas caigan en segundos de reloj distintos (la
    # resolución de _filename_for es de un segundo).
    now = datetime.now()
    with patch("raillytics.ingesta.download.datetime") as mock_datetime:
        mock_datetime.now.side_effect = [now, now + timedelta(seconds=1)]
        first = download(SOURCE, tmp_path)
        second = download(SOURCE, tmp_path)

    assert first != second
    assert first.name != second.name
