from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import requests

from raillytics.ingesta.sources import DataSource

_TIMEOUT_SECONDS = 30


def download(source: DataSource, dest_root: Path) -> Path:
    response = requests.get(source.url, timeout=_TIMEOUT_SECONDS)
    response.raise_for_status()

    dest_dir = dest_root / source.id
    dest_dir.mkdir(parents=True, exist_ok=True)

    filename = _filename_for(source, response)
    dest_path = dest_dir / filename

    # Escritura a fichero temporal + rename atómico: si la descarga se corta a
    # medias, Spark nunca llega a ver un fichero incompleto en staging.
    # El prefijo "." (no solo el sufijo ".tmp") es importante: el listado de
    # ficheros de streaming de Spark solo filtra nombres que empiezan por "."
    # o "_", no sufijos, así que un fichero a medias con solo sufijo ".tmp"
    # podría llegar a ingerirse como si fuera un dato real.
    tmp_path = dest_dir / f".{dest_path.name}.tmp"
    tmp_path.write_bytes(response.content)
    tmp_path.rename(dest_path)

    return dest_path


def _filename_for(source: DataSource, response: requests.Response) -> str:
    # Siempre se antepone un timestamp: el nombre de Content-Disposition es
    # constante entre descargas del mismo origen (verificado contra la URL
    # real de CRTM), y la fuente de streaming por ficheros de Spark recuerda
    # las rutas ya vistas, no el contenido -- sin el timestamp, cada descarga
    # posterior a la primera se saltaría en silencio para siempre.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = _sanitized_content_disposition_name(response)
    if base_name is None:
        base_name = f"{source.id}.{source.format}"
    return f"{timestamp}_{base_name}"


def _sanitized_content_disposition_name(response: requests.Response) -> str | None:
    content_disposition = response.headers.get("Content-Disposition", "")
    if "filename=" not in content_disposition:
        return None

    filename_part = content_disposition.split("filename=")[-1]
    # Descartar parámetros posteriores (p.ej. "; size=6042") antes de quitar comillas.
    filename_part = filename_part.split(";")[0].strip('"; ')
    filename_part = filename_part.replace("\\", "/")

    if ".." in filename_part or filename_part.startswith("/"):
        return None

    basename = os.path.basename(filename_part)
    if not basename or basename in (".", ".."):
        return None

    return basename
