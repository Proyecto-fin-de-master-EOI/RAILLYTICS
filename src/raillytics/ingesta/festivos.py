"""Ingesta Bronze: calendario de festivos laborales (BOE).

El BOE no ofrece los festivos como dataset estructurado (CSV/JSON): cada año
publica una Resolución de la Dirección General de Trabajo con la relación de
fiestas laborales (nacional, autonómica, Ceuta y Melilla) como texto legal.
Se captura el XML oficial de cada Resolución tal cual (capa Bronze, sin
transformar); el parseo de la tabla de festivos a partir del texto se hace en
la capa Silver (PySpark), donde se decide cómo tratar esta heterogeneidad.

Para añadir un año nuevo: buscar en boe.es la Resolución "por la que se
publica la relación de fiestas laborales para el año XXXX" (se publica en
octubre del año anterior) y añadir su identificador BOE-A-YYYY-NNNNN abajo.
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

from raillytics.utils.http import download_file

load_dotenv()

DATA_LAKE_PATH = Path(os.getenv("DATA_LAKE_PATH", "./data"))
BRONZE_FESTIVOS = DATA_LAKE_PATH / "bronze" / "festivos"

BOE_XML_URL = "https://www.boe.es/diario_boe/xml.php?id={boe_id}"

# Resoluciones anuales de fiestas laborales localizadas (histórico disponible).
RESOLUCIONES_FESTIVOS = {
    2024: "BOE-A-2023-22014",
    2025: "BOE-A-2024-21316",
    2026: "BOE-A-2025-21667",
}


def ingestar(fecha: date | None = None) -> list[Path]:
    fecha = fecha or date.today()
    particion = f"fecha_descarga={fecha.isoformat()}"
    descargados = []

    for anio, boe_id in RESOLUCIONES_FESTIVOS.items():
        url = BOE_XML_URL.format(boe_id=boe_id)
        destino = BRONZE_FESTIVOS / particion / f"festivos_{anio}_{boe_id}.xml"
        descargados.append(download_file(url, destino))

    return descargados


if __name__ == "__main__":
    ingestar()
