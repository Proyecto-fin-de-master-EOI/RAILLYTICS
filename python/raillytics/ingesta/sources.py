from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

SUPPORTED_FORMATS = {"csv", "json"}


@dataclass(frozen=True)
class DataSource:
    id: str
    name: str
    url: str
    format: str


def load_sources(path: Path) -> list[DataSource]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    sources = []
    for entry in raw["sources"]:
        fmt = entry["format"]
        if fmt not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Formato no soportado: '{fmt}' (soportados: {sorted(SUPPORTED_FORMATS)})"
            )
        sources.append(
            DataSource(id=entry["id"], name=entry["name"], url=entry["url"], format=fmt)
        )
    return sources
