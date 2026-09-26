from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import datetime

from raillytics.ingesta.sources import DataSource


def staging_filename(source: DataSource, headers: Mapping[str, str]) -> str:
    # Siempre se antepone un timestamp: el nombre de Content-Disposition es
    # constante entre descargas del mismo origen (verificado contra la URL
    # real de CRTM), y la fuente de streaming por ficheros de Spark recuerda
    # las rutas ya vistas, no el contenido -- sin el timestamp, cada descarga
    # posterior a la primera se saltaría en silencio para siempre.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = sanitized_content_disposition_name(headers.get("Content-Disposition", ""))
    if base_name is None:
        base_name = f"{source.id}.{source.format}"
    return f"{timestamp}_{base_name}"


def sanitized_content_disposition_name(content_disposition: str) -> str | None:
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
