from pathlib import Path

import pytest

from raillytics.ingesta.sources import DataSource, load_sources

FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_load_sources_parses_valid_yaml():
    sources = load_sources(FIXTURES / "data_sources_sample.yml")

    assert sources == [
        DataSource(
            id="crtm",
            name="CRTM - Consorcio Regional de Transportes de Madrid",
            url="https://crtm.maps.arcgis.com/sharing/rest/content/items/1a25440bf66f499bae2657ec7fb40144/data",
            format="csv",
        )
    ]


def test_load_sources_rejects_unsupported_format(tmp_path):
    bad_yaml = tmp_path / "bad.yml"
    bad_yaml.write_text(
        "sources:\n"
        "  - id: bad\n"
        "    name: Bad source\n"
        "    url: https://example.invalid/data\n"
        "    format: xlsx\n"
    )

    with pytest.raises(ValueError, match="Formato no soportado"):
        load_sources(bad_yaml)
