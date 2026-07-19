"""Ingesta Bronze: INE — población (API JSON Tempus3, sin necesidad de API key).

Referencia: https://www.ine.es/dyngs/DAB/index.htm?cid=1100

La tabla 2853 (Cifras Oficiales de Población / Revisión del Padrón Municipal)
está descontinuada por el INE desde el año de referencia 2021: a partir de esa
fecha el padrón anual fue sustituido por la Estadística Continua de Población
(ECP), de periodicidad trimestral. Se descarga la serie ECP4961 ("Total
Nacional. Total. Todas las edades. Total. Población. Número."), la serie
equivalente dentro de la ECP, que sí llega hasta el trimestre más reciente.

Pendiente: localizar la serie/tabla equivalente desglosada por provincia para
el cruce de movilidad/población que describe el diseño del proyecto (G3.pdf).
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

# Serie INE ECP4961 (Estadística Continua de Población): población total
# nacional por trimestre (Tempus3 DATOS_SERIE). nult=40 pide los últimos 40
# trimestres (~10 años), en línea con la profundidad histórica que se pedía
# antes a la tabla anual 2853 (nult=10 años).
POBLACION_NACIONAL_URL = "https://servicios.ine.es/wstempus/js/ES/DATOS_SERIE/ECP4961?nult=40"


def ingestar(fecha: date | None = None) -> list[Path]:
    fecha = fecha or date.today()
    particion = f"fecha_descarga={fecha.isoformat()}"

    destino = BRONZE_INE / particion / "poblacion_nacional_ecp4961.json"
    return [download_file(POBLACION_NACIONAL_URL, destino)]


if __name__ == "__main__":
    ingestar()
