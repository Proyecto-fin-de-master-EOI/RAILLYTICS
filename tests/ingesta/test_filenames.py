import re

import pytest

from raillytics.ingesta.filenames import sanitized_content_disposition_name, staging_filename
from raillytics.ingesta.sources import DataSource

SOURCE = DataSource(id="crtm", name="CRTM test", url="https://example.invalid", format="csv")


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ('attachment; filename="crtm_export.csv"', "crtm_export.csv"),
        ('attachment; filename="google_transit_M5.zip"; size=6042', "google_transit_M5.zip"),
        ('attachment; filename="../../../etc/passwd"', None),
        ('attachment; filename="/etc/passwd"', None),
        ("attachment", None),
        ("", None),
    ],
)
def test_sanitized_content_disposition_name(header, expected):
    assert sanitized_content_disposition_name(header) == expected


def test_staging_filename_falls_back_to_source_id_and_format():
    name = staging_filename(SOURCE, {})

    assert re.fullmatch(r"\d{8}_\d{6}_crtm\.csv", name)
