"""Ingesta Bronze: AEMET OpenData — valores climatológicos diarios.

Requiere una API key gratuita (AEMET_API_KEY en .env). Solicitud en:
https://opendata.aemet.es/centrodedescargas/altaUsuario

La API de AEMET funciona en dos pasos: la petición inicial devuelve un JSON
con una URL temporal en el campo "datos", y es esa URL la que sirve el
histórico climatológico real. Se capturan ambas respuestas tal cual en Bronze.
"""
from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

from raillytics.utils.http import download_file

load_dotenv()

logger = logging.getLogger("raillytics.ingesta")

DATA_LAKE_PATH = Path(os.getenv("DATA_LAKE_PATH", "./data"))
BRONZE_AEMET = DATA_LAKE_PATH / "bronze" / "aemet"

BASE_URL = (
    "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/"
    "fechaini/{fecha_ini}T00:00:00UTC/fechafin/{fecha_fin}T23:59:59UTC/todasestaciones"
)


def ingestar(fecha_ini: date, fecha_fin: date, fecha: date | None = None) -> list[Path]:
    """Descarga el histórico climatológico diario entre fecha_ini y fecha_fin.

    AEMET limita cada petición a un rango corto (~15 días); para históricos
    más largos hay que llamar a esta función en tramos.
    """
    api_key = os.getenv("AEMET_API_KEY")
    if not api_key:
        logger.warning(
            "AEMET_API_KEY no configurada en .env. Solicítala en "
            "https://opendata.aemet.es/centrodedescargas/altaUsuario y añádela "
            "al .env para poder ejecutar esta ingesta."
        )
        return []

    fecha = fecha or date.today()
    particion = f"fecha_descarga={fecha.isoformat()}"

    url = BASE_URL.format(fecha_ini=fecha_ini.isoformat(), fecha_fin=fecha_fin.isoformat())
    respuesta = requests.get(url, params={"api_key": api_key}, timeout=30)
    respuesta.raise_for_status()
    metadata = respuesta.json()

    if metadata.get("estado") != 200:
        logger.error("AEMET devolvió un error: %s", metadata.get("descripcion"))
        return []

    rango = f"{fecha_ini.isoformat()}_{fecha_fin.isoformat()}"
    destino_metadata = BRONZE_AEMET / particion / f"metadata_{rango}.json"
    destino_metadata.parent.mkdir(parents=True, exist_ok=True)
    destino_metadata.write_text(respuesta.text, encoding="utf-8")

    destino_datos = BRONZE_AEMET / particion / f"valores_climatologicos_{rango}.json"
    return [download_file(metadata["datos"], destino_datos)]


if __name__ == "__main__":
    hoy = date.today()
    ingestar(fecha_ini=hoy - timedelta(days=15), fecha_fin=hoy)
