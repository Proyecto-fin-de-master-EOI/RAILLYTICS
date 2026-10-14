"""Meteorología observada de las dos cabeceras del corredor, desde AEMET.

La ingesta diaria pide a AEMET el último tramo disponible, que es lo que necesita el DAG. Para
tener la serie hacia atrás hay que recorrer tramos: AEMET solo sirve 15 días por petición, así que
la ventana del proyecto son más de treinta peticiones por estación. Eso es una carga inicial, no
parte del pipeline diario.

Las URL, la credencial y los reintentos del enlace temporal no se duplican: salen de las fuentes
`aemet_*` de config/data_sources.yml y de `downloaders.aemet_ventana`, el mismo camino que usa la
ingesta.

Cada tramo descargado se guarda tal cual en disco y no se vuelve a pedir: AEMET es gratuita y con
límite de peticiones, así que relanzar esto tras un corte no debe castigar a la API. Borrar la
caché fuerza la descarga otra vez.

Uso:
    python -m raillytics.ingesta.aemet_historico --desde 2025-06-01
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv

from raillytics.ingesta import referencia
from raillytics.ingesta.downloaders import AEMET_DIAS_RETARDO, AEMET_DIAS_VENTANA, aemet_ventana
from raillytics.ingesta.sources import DataSource, load_sources

logger = logging.getLogger(__name__)

PREFIJO_FUENTE = "aemet_"

# Qué cabecera del corredor es cada fuente. Explícito y no deducido del nombre de la estación: si
# se añade otra estación hay que decir a qué ciudad corresponde, no adivinarlo.
CIUDAD_POR_FUENTE = {
    "aemet_madrid_barajas": "MAD",
    "aemet_barcelona_prat": "BCN",
}

# AEMET limita las peticiones por minuto y contesta 429 cuando se pasa. Cada tramo son DOS
# peticiones (la del enlace temporal y la de los datos), así que la pausa se calcula sobre eso: con
# 4 segundos salen unas 30 peticiones por minuto, que se queda holgada por debajo del límite.
PAUSA_SEGUNDOS = 4.0
# Aun así puede contestar 429 (p. ej. si algo más está usando la misma clave): se espera y se
# reintenta, porque un 429 NO significa que el tramo no exista.
REINTENTOS_LIMITE = 4
ESPERA_LIMITE_SEGUNDOS = 60

# AEMET marca con «Ip» la precipitación inapreciable (por debajo de 0,1 mm). Es un cero medido, no
# un dato que falte, así que se convierte en 0,0 en vez de dejarlo nulo.
PRECIPITACION_INAPRECIABLE = "Ip"


def _numero(valor: object) -> float | None:
    """Un número de AEMET: vienen como texto y con coma decimal («27,4»).

    Devuelve None cuando el dato no está, para no inventar un cero donde no hubo medida.
    """
    if valor is None:
        return None
    texto = str(valor).strip()
    if not texto:
        return None
    if texto == PRECIPITACION_INAPRECIABLE:
        return 0.0
    try:
        return float(texto.replace(",", "."))
    except ValueError:
        # AEMET usa alguna marca más en series especiales (p. ej. «Acum»): mejor sin dato que un
        # número inventado.
        logger.debug("valor de AEMET no numérico, se deja sin dato: %r", valor)
        return None


def ventanas(desde: date, hasta: date, dias: int = AEMET_DIAS_VENTANA) -> list[tuple[date, date]]:
    """Trocea el rango en tramos del tamaño máximo que admite una petición de AEMET."""
    tramos = []
    ini = desde
    while ini <= hasta:
        fin = min(ini + timedelta(days=dias - 1), hasta)
        tramos.append((ini, fin))
        ini = fin + timedelta(days=1)
    return tramos


def _espera_tras_limite(respuesta: requests.Response, intento: int) -> int:
    """Segundos a esperar tras un 429: lo que diga Retry-After o, si no lo dice, la ventana del límite."""
    cabecera = (respuesta.headers.get("Retry-After") or "").strip()
    return int(cabecera) if cabecera.isdigit() else ESPERA_LIMITE_SEGUNDOS * intento


def _pedir(fuente: DataSource, ini: date, fin: date) -> bytes:
    """Un tramo de AEMET, esperando y reintentando si contesta 429.

    Importa distinguirlo de un tramo que no existe: tratar el 429 como «no hay datos» deja huecos
    en la serie que parecen del origen y son nuestros.
    """
    for intento in range(1, REINTENTOS_LIMITE + 1):
        try:
            return aemet_ventana(fuente, ini, fin).content
        except requests.HTTPError as exc:
            respuesta = exc.response
            if respuesta is None or respuesta.status_code != 429 or intento == REINTENTOS_LIMITE:
                raise
            espera = _espera_tras_limite(respuesta, intento)
            logger.info("  AEMET ha limitado las peticiones: se espera %ds y se reintenta (%d/%d)",
                        espera, intento, REINTENTOS_LIMITE)
            time.sleep(espera)
    raise AssertionError("inalcanzable")  # pragma: no cover


def _tramo(fuente: DataSource, ini: date, fin: date, cache: Path) -> list[dict]:
    """Registros de un tramo, de la caché si ya está en disco o de AEMET si no."""
    fichero = cache / f"{ini.isoformat()}_{fin.isoformat()}.json"
    if fichero.exists() and fichero.stat().st_size > 0:
        return json.loads(fichero.read_text(encoding="utf-8"))

    contenido = _pedir(fuente, ini, fin)
    registros = json.loads(contenido)
    cache.mkdir(parents=True, exist_ok=True)
    fichero.write_bytes(contenido)
    time.sleep(PAUSA_SEGUNDOS)
    return registros


def construir(registro: Path, desde: date, hasta: date, cache: Path) -> pd.DataFrame:
    """Serie diaria de temperatura y precipitación de las cabeceras del corredor."""
    fuentes = [f for f in load_sources(registro) if f.id.startswith(PREFIJO_FUENTE)]
    if not fuentes:
        raise ValueError(f"no hay ninguna fuente '{PREFIJO_FUENTE}*' en {registro}")
    desconocidas = sorted(f.id for f in fuentes if f.id not in CIUDAD_POR_FUENTE)
    if desconocidas:
        raise ValueError(
            f"no se sabe a qué ciudad corresponden estas fuentes de AEMET: {', '.join(desconocidas)} "
            f"(declararlas en CIUDAD_POR_FUENTE)"
        )

    filas: list[dict] = []
    for fuente in sorted(fuentes, key=lambda f: f.id):
        ciudad = CIUDAD_POR_FUENTE[fuente.id]
        tramos = ventanas(desde, hasta)
        logger.info("%s (%s): %d tramos entre %s y %s", fuente.id, ciudad, len(tramos), desde, hasta)
        dias = 0
        for i, (ini, fin) in enumerate(tramos, 1):
            try:
                registros = _tramo(fuente, ini, fin, cache / fuente.id)
            except Exception as exc:  # un tramo sin datos no debe tirar una carga de minutos
                logger.warning("  %s %s..%s: no se ha podido traer (%s), se omite", fuente.id, ini, fin, exc)
                continue
            for r in registros:
                if not r.get("fecha"):
                    continue
                filas.append({
                    "fecha": date.fromisoformat(str(r["fecha"])[:10]),
                    "ciudad": ciudad,
                    "temperatura_media": _numero(r.get("tmed")),
                    "precipitacion_mm": _numero(r.get("prec")),
                })
                dias += 1
            if i % 10 == 0:
                logger.info("  %s: %d/%d tramos", fuente.id, i, len(tramos))
        logger.info("  %s: %d días", fuente.id, dias)

    df = pd.DataFrame(filas, columns=["fecha", "ciudad", "temperatura_media", "precipitacion_mm"])
    # El contrato de la predicción exige UNA fila por (fecha, ciudad). Los tramos de AEMET no se
    # solapan, pero si se cambia el tamaño de ventana con la caché llena podrían repetirse días.
    antes = len(df)
    df = df.drop_duplicates(["fecha", "ciudad"], keep="last")
    if len(df) != antes:
        logger.warning("descartadas %d filas repetidas de (fecha, ciudad)", antes - len(df))
    return df.sort_values(["fecha", "ciudad"], ignore_index=True)


def main(argv: list[str] | None = None) -> int:
    # Igual que nap_historico: lanzado por `make` la credencial ya viene exportada, pero cargando
    # el .env también funciona llamándolo a mano.
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Descarga el histórico de meteorología de AEMET.")
    p.add_argument("--desde", type=date.fromisoformat, default=date(2025, 6, 1))
    # AEMET publica con retardo: pedir hasta hoy devuelve tramos vacíos.
    p.add_argument("--hasta", type=date.fromisoformat,
                   default=date.today() - timedelta(days=AEMET_DIAS_RETARDO))
    p.add_argument("--registro", type=Path, default=Path("config/data_sources.yml"))
    p.add_argument("--cache", type=Path, default=Path("data/historico/aemet"))
    args = p.parse_args(argv)

    df = construir(args.registro, args.desde, args.hasta, args.cache)
    if df.empty:
        logger.error("no se ha podido construir ningún registro")
        return 1
    destino = referencia.escribir(
        "meteo", df, {"desde": str(args.desde), "hasta": str(args.hasta)}, os.environ, __name__
    )

    logger.info("meteo: %d filas en %s", len(df), destino)
    for ciudad, g in df.groupby("ciudad"):
        logger.info(
            "  %s: %d días, %s .. %s, tmed media %.1f C, días con lluvia %d",
            ciudad, len(g), g["fecha"].min(), g["fecha"].max(),
            g["temperatura_media"].mean(), int((g["precipitacion_mm"] > 0).sum()),
        )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
