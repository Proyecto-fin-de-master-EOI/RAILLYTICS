"""Orquestador de la ingesta Bronze: ejecuta todas las fuentes.

Uso: python -m raillytics.ingesta.run_ingesta
"""
from __future__ import annotations

from datetime import date, timedelta

from raillytics.ingesta import aemet, festivos, ine, renfe


def main() -> None:
    renfe.ingestar()
    festivos.ingestar()
    ine.ingestar()

    hoy = date.today()
    aemet.ingestar(fecha_ini=hoy - timedelta(days=15), fecha_fin=hoy)


if __name__ == "__main__":
    main()
