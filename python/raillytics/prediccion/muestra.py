"""Fuentes SINTÉTICAS de la predicción de demanda, en las rutas y con las columnas que asume config/prediccion.yml.

Mientras los DAGs de Airflow no ingesten las fuentes reales (demanda trimestral, festivos, eventos y meteo), este
módulo genera las cuatro de forma determinista como Parquet en Bronze L2, para poder ejecutar `make 06_prediccion` y
`make 00_ingest` de punta a punta. LOS DATOS NO SON REALES: la demanda sigue un modelo simple (nivel, estacionalidad
trimestral y crecimiento anual) y los eventos y la meteo son inventados (los eventos llevan «(muestra)» en el nombre).
Van a prefijos PROPIOS (`muestra_<fuente>`), nunca a los de las fuentes reales: si compartieran prefijo, cuando Airflow
ingeste las reales la consulta (`*/*.parquet`) leería ambas a la vez y los totales saldrían duplicados sin ningún error.
Cuando existan las reales, se cambia en config/prediccion.yml la ruta `muestra_<fuente>` por la de cada fuente real.

    <bronze>/l2/muestra_<fuente>/sintetico/<fuente>.parquet      fuentes: demanda_trimestral, festivos, eventos, aemet

Uso:  python -m raillytics.prediccion.muestra [--hasta AAAA-Tn] [--trimestres N] [--semilla N]
"""
from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Mapping, Sequence
from datetime import date, timedelta

import duckdb
import numpy as np
import pandas as pd
from dotenv import find_dotenv, load_dotenv

from raillytics.prediccion.entradas import raiz_bronze
from raillytics.prediccion.salida import CORREDOR
from raillytics.prediccion.servicio import conectar
from raillytics.prediccion.trimestre import Trimestre
from raillytics.procesamiento.silver_sample import festivos_nacionales
from raillytics.utils.cargas import registrar_carga
from raillytics.utils.lake import LakeLayout, ensure_parent_dir

logger = logging.getLogger(__name__)

FUENTES = ("demanda_trimestral", "festivos", "eventos", "aemet")

# Nivel base por trimestre (viajeros, ambos sentidos), estacionalidad por número de trimestre y crecimiento anual.
BASE = {CORREDOR: 2_400_000, "AVE-MAD-SEV": 900_000}  # el segundo es un corredor «distractor»: la consulta debe filtrarlo
ESTACIONALIDAD = {1: 0.94, 2: 1.04, 3: 1.10, 4: 0.98}
CRECIMIENTO_ANUAL = 0.04
TRIMESTRES_POR_DEFECTO = 11


def demanda_trimestral(hasta: Trimestre, trimestres: int, semilla: int) -> pd.DataFrame:
    """Viajeros por trimestre, corredor y sentido: los `trimestres` últimos hasta `hasta` (incluido)."""
    azar = np.random.default_rng(semilla)
    filas = []
    for i in range(trimestres):
        trimestre = hasta.menos(trimestres - 1 - i)
        for corredor, base in BASE.items():
            total = base * ESTACIONALIDAD[trimestre.numero] * (1 + CRECIMIENTO_ANUAL) ** (i / 4) * (1 + azar.normal(0, 0.015))
            total = int(round(total))
            ida = int(round(total * azar.uniform(0.49, 0.52)))
            filas.append((trimestre.anio, trimestre.numero, corredor, "ida", ida))
            filas.append((trimestre.anio, trimestre.numero, corredor, "vuelta", total - ida))
    return pd.DataFrame(filas, columns=["anio", "trimestre", "corredor", "sentido", "viajeros"])


def festivos(anio_desde: int, anio_hasta: int) -> pd.DataFrame:
    """Festivos nacionales de España (los mismos que usa el Silver sintético)."""
    filas = [
        (dia, nombre)
        for anio in range(anio_desde, anio_hasta + 1)
        for dia, nombre in sorted(festivos_nacionales(anio).items())
    ]
    return pd.DataFrame(filas, columns=["fecha", "nombre"])


def _dia_entre(azar: np.random.Generator, anio: int, desde: tuple[int, int], hasta: tuple[int, int]) -> date:
    inicio = date(anio, *desde)
    return inicio + timedelta(days=int(azar.integers(0, (date(anio, *hasta) - inicio).days + 1)))


def eventos(anio_desde: int, anio_hasta: int, semilla: int) -> pd.DataFrame:
    """Eventos inventados en Madrid y Barcelona. Todos acaban en «(muestra)» para no confundirlos con datos reales."""
    azar = np.random.default_rng(semilla + 2)
    filas = []
    for anio in range(anio_desde, anio_hasta + 1):
        filas.append((_dia_entre(azar, anio, (10, 15), (11, 30)), "Partido de liga Real Madrid – FC Barcelona (muestra)", "MAD"))
        filas.append((_dia_entre(azar, anio, (3, 5), (4, 20)), "Partido de liga FC Barcelona – Real Madrid (muestra)", "BCN"))
        filas.append((_dia_entre(azar, anio, (4, 15), (5, 20)), "Final de Copa (muestra)", str(azar.choice(["MAD", "BCN"]))))
        filas.extend((date(anio, 3, 2) + timedelta(days=d), "Congreso profesional (muestra)", "BCN") for d in range(4))
        for _ in range(6):
            filas.append((_dia_entre(azar, anio, (1, 1), (12, 31)), "Concierto grande (muestra)", str(azar.choice(["MAD", "BCN"]))))
    return pd.DataFrame(filas, columns=["fecha", "descripcion", "ciudad"])


def meteo(desde: date, hasta: date, semilla: int) -> pd.DataFrame:
    """Temperatura media y precipitación diarias observadas en Madrid y Barcelona (ciclo anual + ruido)."""
    azar = np.random.default_rng(semilla + 3)
    dias = pd.date_range(desde, hasta, freq="D")
    dia_del_anio = dias.dayofyear.to_numpy()
    mes = dias.month.to_numpy()
    tablas = []
    for ciudad, media, amplitud, ruido in (("MAD", 15.5, 10.5, 2.5), ("BCN", 17.5, 7.5, 2.0)):
        temperatura = media + amplitud * np.sin(2 * np.pi * (dia_del_anio - 110) / 365) + azar.normal(0, ruido, len(dias))
        llueve = azar.random(len(dias)) < (0.10 + 0.08 * np.isin(mes, (4, 5, 10, 11)))
        precipitacion = np.where(llueve, azar.exponential(6.0, len(dias)), 0.0)
        tablas.append(
            pd.DataFrame({"fecha": dias, "ciudad": ciudad, "tmed": temperatura.round(1), "prec": precipitacion.round(1)})
        )
    return pd.concat(tablas, ignore_index=True)


def generar_muestra(hasta: Trimestre, trimestres: int = TRIMESTRES_POR_DEFECTO, semilla: int = 42) -> dict[str, pd.DataFrame]:
    """Las cuatro fuentes. Festivos y eventos llegan hasta un año después del último trimestre (para poder predecir)."""
    primero = hasta.menos(trimestres - 1)
    return {
        "demanda_trimestral": demanda_trimestral(hasta, trimestres, semilla),
        "festivos": festivos(primero.anio, hasta.mas(4).anio),
        "eventos": eventos(primero.anio, hasta.mas(4).anio, semilla),
        "aemet": meteo(primero.inicio, hasta.fin, semilla),
    }


def escribir_muestra(
    con: duckdb.DuckDBPyConnection,
    layout: LakeLayout,
    bronze_root: str,
    muestra: Mapping[str, pd.DataFrame],
    parametros: Mapping[str, object],
) -> dict[str, str]:
    """Escribe cada fuente como un Parquet en su prefijo `muestra_<fuente>` de Bronze L2 (sobrescribe: es idempotente) y lo registra."""
    escritos: dict[str, str] = {}
    with registrar_carga("prediccion_muestra", "bronze", layout, con, parametros=parametros) as ejecucion:
        for fuente, tabla in muestra.items():
            destino = f"{bronze_root}/l2/muestra_{fuente}/sintetico/{fuente}.parquet"
            with ejecucion.tabla(fuente, origen=__name__, destino=destino) as carga:
                ensure_parent_dir(destino)
                # pandas guarda las fechas como timestamps de nanosegundos; el contrato es DATE, como lo escribiría Spark.
                seleccion = "* REPLACE (CAST(fecha AS DATE) AS fecha)" if "fecha" in tabla.columns else "*"
                con.register("muestra_frame", tabla)
                con.execute(f"COPY (SELECT {seleccion} FROM muestra_frame) TO '{destino}' (FORMAT PARQUET)")
                con.unregister("muestra_frame")
                carga.filas = len(tabla)
            escritos[fuente] = destino
            logger.info("%s: %d filas -> %s", fuente, len(tabla), destino)
    return escritos


def _trimestre(texto: str) -> Trimestre:
    try:
        return Trimestre.parse(texto)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _entero_positivo(texto: str) -> int:
    try:
        valor = int(texto)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{texto!r} no es un entero") from exc
    if valor <= 0:
        raise argparse.ArgumentTypeError("debe ser un entero positivo")
    return valor


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m raillytics.prediccion.muestra",
        description="Genera fuentes SINTÉTICAS de la predicción de demanda (demanda trimestral, festivos, eventos y meteo) en Bronze L2",
    )
    parser.add_argument("--hasta", type=_trimestre, help="último trimestre publicado (AAAA-Tn); por defecto, el trimestre anterior al en curso")
    parser.add_argument("--trimestres", type=_entero_positivo, default=TRIMESTRES_POR_DEFECTO, help=f"cuántos trimestres publicados (por defecto {TRIMESTRES_POR_DEFECTO}; hacen falta ≥ 5)")
    parser.add_argument("--semilla", type=int, default=42, help="semilla: mismos argumentos y semilla producen los mismos datos")
    args = parser.parse_args(argv)
    if env is None:
        load_dotenv(find_dotenv(usecwd=True))
        env = os.environ

    hasta = args.hasta or Trimestre.de_fecha(date.today()).menos(1)
    layout = LakeLayout.from_env(env)
    bronze = raiz_bronze(env)
    muestra = generar_muestra(hasta, args.trimestres, args.semilla)
    parametros = {"hasta": str(hasta), "trimestres": args.trimestres, "semilla": args.semilla}
    escritos = escribir_muestra(conectar(layout, env), layout, bronze, muestra, parametros)

    print(f"Fuentes SINTÉTICAS de la predicción (NO son datos reales) hasta {hasta}, semilla {args.semilla} -> {bronze}/l2/muestra_*/")
    for fuente, ruta in escritos.items():
        print(f"  {fuente:<20}{len(muestra[fuente]):>8,} filas  {ruta}")
    print(f"Ahora:  make 06_prediccion TRIMESTRE={hasta.mas(1)}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    raise SystemExit(main())
