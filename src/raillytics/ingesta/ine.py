"""Ingesta Bronze: INE — población (API JSON Tempus3, sin necesidad de API key).

Referencia: https://www.ine.es/dyngs/DAB/index.htm?cid=1100

De momento se descarga la tabla nacional de población (2853). Pendiente:
localizar la tabla equivalente desglosada por provincia para el cruce de
movilidad/población que describe el diseño del proyecto (G3.pdf).
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

from raillytics.utils.http import download_file

load_dotenv()

DATA_LAKE_PATH = Path(os.getenv("DATA_LAKE_PATH", "./data"))
BRONZE_INE = DATA_LAKE_PATH / "bronze" / "ine"

# Tabla INE 2853: población total nacional por año (Tempus3 DATOS_TABLA).
POBLACION_NACIONAL_URL = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/2853?nult=10"


def ingestar(fecha: date | None = None) -> list[Path]:
    fecha = fecha or date.today()
    particion = f"fecha_descarga={fecha.isoformat()}"

    destino = BRONZE_INE / particion / "poblacion_nacional_tabla2853.json"
    return [download_file(POBLACION_NACIONAL_URL, destino)]


if __name__ == "__main__":
    ingestar()
