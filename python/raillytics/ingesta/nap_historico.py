"""Carga histórica del NAP: descarga los GTFS pasados de un conjunto de datos.

La fuente `nap_gtfs_*` del registro trae **el último snapshot**, que es lo que necesita la
ingesta diaria. Para reconstruir la serie de oferta hacia atrás hace falta el histórico, y eso
es un proceso aparte: se lanza una vez (o cuando se amplía la ventana), no cada día.

Cada snapshot son tres peticiones: listar el conjunto, pedir el enlace del fichero y bajarlo de
la URL firmada de S3. Es idempotente: si el ZIP ya está en disco no se vuelve a pedir, así que
se puede relanzar tras un corte sin repetir trabajo.

Uso:
    python -m raillytics.ingesta.nap_historico --desde 2025-06-01
"""
from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date
from pathlib import Path

import requests
from dotenv import find_dotenv, load_dotenv

logger = logging.getLogger(__name__)

BASE = "https://nap.transportes.gob.es/api/v2"
TIMEOUT = 120
# Pausa entre snapshots: cortesía con una API pública y gratuita.
PAUSA_SEGUNDOS = 0.4

# Conjuntos de datos ferroviarios del NAP. Iryo no publica GTFS, así que la oferta cubre
# Renfe y OUIGO pero no el corredor completo.
CONJUNTOS = {
    897: "renfe",   # Renfe: Media, Larga Distancia y AVE. Histórico desde 2017-02.
    1515: "ouigo",  # OUIGO. Histórico desde 2023-06.
}


def _cabeceras() -> dict[str, str]:
    clave = os.getenv("NAP_API_KEY")
    if not clave:
        raise RuntimeError(
            "Falta NAP_API_KEY en el .env. Se pide en nap.transportes.gob.es -> Editar Perfil."
        )
    # La API espera la clave en la cabecera `ApiKey`, no como Bearer.
    return {"ApiKey": clave, "accept": "application/json"}


def listar(conjunto: int) -> list[dict]:
    """Snapshots de un conjunto, ordenados por fecha de publicación."""
    r = requests.get(f"{BASE}/conjunto-dato/{conjunto}/historico", headers=_cabeceras(), timeout=TIMEOUT)
    r.raise_for_status()
    datos = [s for s in r.json().get("data", []) if s.get("fecha") and s.get("id")]
    return sorted(datos, key=lambda s: str(s["fecha"]))


def _enlace(snapshot_id: int) -> tuple[str, str] | None:
    """URL firmada y nombre del fichero de un snapshot, o None si el NAP no lo sirve.

    OJO: el `enlaceDescarga` que devuelve el listado trae los dos últimos segmentos invertidos
    (`/historico/descarga`) y da 404. El orden bueno es `/descarga/historico`.
    """
    r = requests.get(f"{BASE}/fichero/{snapshot_id}/descarga/historico", headers=_cabeceras(), timeout=TIMEOUT)
    r.raise_for_status()
    datos = r.json().get("data") or {}
    url = datos.get("enlaceDescarga")
    return (url, datos.get("nombreFichero") or f"{snapshot_id}.zip") if url else None


def descargar(conjunto: int, etiqueta: str, destino: Path, desde: date, hasta: date) -> list[Path]:
    """Baja los snapshots publicados entre dos fechas. No repite los que ya están en disco."""
    destino.mkdir(parents=True, exist_ok=True)
    snapshots = [s for s in listar(conjunto) if desde.isoformat() <= str(s["fecha"])[:10] <= hasta.isoformat()]
    logger.info("%s: %d snapshots entre %s y %s", etiqueta, len(snapshots), desde, hasta)

    bajados: list[Path] = []
    for i, s in enumerate(snapshots, 1):
        # El nombre lleva la fecha de publicación por delante: así ordenan solos y se sabe
        # a qué snapshot corresponde cada fichero sin abrirlo.
        fichero = destino / f"{str(s['fecha'])[:10]}_{s['id']}.zip"
        if fichero.exists() and fichero.stat().st_size > 0:
            bajados.append(fichero)
            continue
        try:
            enlace = _enlace(int(s["id"]))
            if not enlace:
                logger.warning("  %s: el snapshot %s no tiene enlace, se omite", etiqueta, s["id"])
                continue
            # La URL firmada ya lleva la autorización en la firma: no se le manda la ApiKey.
            contenido = requests.get(enlace[0], timeout=TIMEOUT).content
            fichero.write_bytes(contenido)
            bajados.append(fichero)
        except Exception as e:  # una caída puntual no debe tirar una descarga de horas
            logger.warning("  %s: falla el snapshot %s (%s), se omite", etiqueta, s["id"], e)
            continue
        if i % 25 == 0:
            logger.info("  %s: %d/%d", etiqueta, i, len(snapshots))
        time.sleep(PAUSA_SEGUNDOS)
    return bajados


def main(argv: list[str] | None = None) -> int:
    # Igual que raillytics.prediccion.__main__: lanzado por `make` las variables ya vienen
    # exportadas, pero cargando el .env también funciona llamándolo a mano.
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Descarga el histórico de GTFS del NAP.")
    p.add_argument("--desde", type=date.fromisoformat, default=date(2025, 6, 1))
    p.add_argument("--hasta", type=date.fromisoformat, default=date.today())
    p.add_argument("--destino", type=Path, default=Path("data/historico/nap"))
    args = p.parse_args(argv)

    total = 0
    for conjunto, etiqueta in CONJUNTOS.items():
        ficheros = descargar(conjunto, etiqueta, args.destino / etiqueta, args.desde, args.hasta)
        tam = sum(f.stat().st_size for f in ficheros)
        logger.info("%s: %d ficheros, %.1f MB", etiqueta, len(ficheros), tam / 1024 / 1024)
        total += len(ficheros)
    logger.info("histórico del NAP: %d ficheros en %s", total, args.destino)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
