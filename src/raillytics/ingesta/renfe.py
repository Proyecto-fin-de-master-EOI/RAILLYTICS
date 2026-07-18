"""Ingesta Bronze: Renfe Open Data (estaciones, viajeros por franja horaria, puntualidad).

Fuente: https://data.renfe.com (portal CKAN). El fichero de estaciones se sirve
desde ssl.renfe.com, cuyo certificado TLS lleva roto de forma persistente desde
finales de 2025 (confirmado en el propio archiver de data.renfe.com); por eso
esa descarga concreta se hace con verify=False. El resto de recursos se sirven
con certificado válido desde data.renfe.com.
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

from raillytics.utils.http import download_file

load_dotenv()

DATA_LAKE_PATH = Path(os.getenv("DATA_LAKE_PATH", "./data"))
BRONZE_RENFE = DATA_LAKE_PATH / "bronze" / "renfe"

# Recursos localizados vía la API CKAN de data.renfe.com (api/3/action/package_show).
ESTACIONES_URL = "https://ssl.renfe.com/ftransit/Fichero_estaciones/estaciones.csv"
PUNTUALIDAD_URL = (
    "https://data.renfe.com/dataset/db368105-75a7-4af9-9784-bf30ed29e22f/resource/"
    "b5e9016c-38bc-4d28-91f1-42d6d2e5b7fa/download/indemnizaciones-por-retrasos.csv"
)
VIAJEROS_FRANJA_URLS = {
    "cercanias_nacional": (
        "https://data.renfe.com/dataset/bad45d4d-b809-40bd-8abb-d7f2c8bc9214/resource/"
        "566dcfa2-4c02-48bf-99c9-a7606a097073/download/viajeros_por_franja_csv.csv"
    ),
    "madrid": (
        "https://data.renfe.com/dataset/57249df2-b1af-485f-bc1c-7436b0622477/resource/"
        "a269b1e5-2760-4296-9db8-3339c3dde005/download/madrid_viajeros_por_franja_csv.csv"
    ),
}


def ingestar(fecha: date | None = None) -> list[Path]:
    fecha = fecha or date.today()
    particion = f"fecha_descarga={fecha.isoformat()}"
    descargados = []

    descargados.append(
        download_file(
            ESTACIONES_URL,
            BRONZE_RENFE / "estaciones" / particion / "estaciones.csv",
            verify=False,
        )
    )
    descargados.append(
        download_file(
            PUNTUALIDAD_URL,
            BRONZE_RENFE / "puntualidad" / particion / "compromiso_puntualidad.csv",
        )
    )
    for demarcacion, url in VIAJEROS_FRANJA_URLS.items():
        descargados.append(
            download_file(url, BRONZE_RENFE / "viajeros_franja" / particion / f"{demarcacion}.csv")
        )

    return descargados


if __name__ == "__main__":
    ingestar()
