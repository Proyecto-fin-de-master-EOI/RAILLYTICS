from __future__ import annotations

from pathlib import Path

import requests

from raillytics.ingesta.filenames import staging_filename
from raillytics.ingesta.sources import DataSource
from raillytics.utils.fs import atomic_write_bytes

_TIMEOUT_SECONDS = 30


def download(source: DataSource, dest_root: Path) -> Path:
    response = requests.get(source.url, timeout=_TIMEOUT_SECONDS)
    response.raise_for_status()

    dest_dir = dest_root / source.id
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest_path = dest_dir / staging_filename(source, response.headers)
    return atomic_write_bytes(dest_path, response.content)
