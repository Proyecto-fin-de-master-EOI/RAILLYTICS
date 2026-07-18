"""Utilidades HTTP compartidas por los scripts de ingesta (capa Bronze)."""
from __future__ import annotations

import logging
from pathlib import Path

import requests

logger = logging.getLogger("raillytics.ingesta")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DEFAULT_TIMEOUT = 30


def download_file(url: str, destino: Path, *, verify: bool = True, timeout: int = DEFAULT_TIMEOUT) -> Path:
    """Descarga ``url`` y la guarda en ``destino``, creando las carpetas necesarias.

    Idempotente: si ``destino`` ya existe, no vuelve a descargar (evita duplicados
    en re-ejecuciones del mismo día, tal como describe la arquitectura del proyecto).
    """
    destino.parent.mkdir(parents=True, exist_ok=True)

    if destino.exists():
        logger.info("Ya existe, se omite descarga: %s", destino)
        return destino

    logger.info("Descargando %s -> %s", url, destino)
    respuesta = requests.get(url, timeout=timeout, verify=verify)
    respuesta.raise_for_status()
    destino.write_bytes(respuesta.content)
    logger.info("OK (%d bytes): %s", len(respuesta.content), destino)
    return destino
