"""Contrasta las reglas de reparto diario contra la oferta real del NAP.

Los coeficientes de `config/reglas_demanda.yml` son hipótesis sobre la forma de la demanda. Aquí se
comprueban contra el único dato diario que sí está medido: cuántos trenes circulan cada día.

Dos salidas:

1. **Los coeficientes que propone la oferta.** Si el operador pone un 23 % menos de trenes el
   domingo que el martes, suponer que ese día se viaja un 27 % MÁS es incompatible con la realidad.
2. **La ocupación que implican las reglas actuales.** Reparto diario (viajeros·km) dividido por la
   capacidad de ese día (plazas·km). Un día por encima del 100 % es imposible, y señala el
   coeficiente que hay que corregir.

Se mide en **viajeros·km / plazas·km**, no en viajeros por plaza: un asiento se vende por tramos,
así que el ratio de cabezas pasa del 100 % legítimamente y no sirve para detectar lo imposible.

Solo entran los días con **cobertura completa**: si a un día le falta un operador, su total no es
menos servicio sino dato incompleto, y contarlo finge un recorte que no existe.

Uso:
    python -m raillytics.prediccion.calibracion
"""
from __future__ import annotations

import argparse
import io
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import find_dotenv, load_dotenv

from raillytics.ingesta.downloaders import obtener
from raillytics.ingesta.referencia import conexion
from raillytics.ingesta.sources import load_sources
from raillytics.prediccion.calendario import construir_calendario
from raillytics.prediccion.reglas import cargar_reglas, indices_base
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.lake import LakeLayout

logger = logging.getLogger(__name__)

CORREDOR = "Madrid-Barcelona"
SEMANA = ("lun", "mar", "mié", "jue", "vie", "sáb", "dom")
# El día de referencia: un martes vale 1.00 en las reglas, así que todo se normaliza contra él.
REFERENCIA = 1
# Operadores del NAP. Iryo no publica GTFS, así que la oferta cubre el corredor pero no entero.
SIN_GTFS = "Iryo"


def oferta_diaria(con, layout, solo_completos: bool = True) -> pd.DataFrame:
    """Trenes al día del corredor, sumando operadores. Por defecto solo los días con todos."""
    filtro = " AND cobertura_completa" if solo_completos else ""
    df = con.execute(
        f"SELECT fecha, sum(trenes) AS trenes FROM read_parquet('{layout.silver_glob('oferta_diaria')}') "
        f"WHERE corredor = '{CORREDOR}'{filtro} GROUP BY 1 ORDER BY 1"
    ).df()
    df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
    df["dia_semana"] = [d.weekday() for d in df["fecha"]]
    return df


def coeficientes(oferta: pd.DataFrame) -> dict[str, float]:
    """Coeficiente por día de la semana que propone la oferta, normalizado contra el martes.

    Se usa la **mediana** y no la media: un día suelto con el GTFS incompleto arrastra la media y
    no la mediana, y en esta serie los hay.
    """
    mediana = oferta.groupby("dia_semana")["trenes"].median()
    ref = mediana.get(REFERENCIA)
    if not ref:
        raise ValueError("no hay días de referencia (martes) con oferta: no se puede normalizar")
    return {SEMANA[d]: round(v / ref, 2) for d, v in mediana.items()}


# Columnas del CSV de CNMC, por si hay que leerlo de la fuente en vez del Silver.
COL_CNMC = {"viajeros_km": "Viajeros.km (Viajeros.km)", "plazas_km": "Plazas.km Ofertadas (Plazas.km)"}


def _cnmc_del_silver(con, layout) -> pd.DataFrame | None:
    try:
        return con.execute(
            f"SELECT anio, trimestre, sum(viajeros_km) AS viajeros_km, sum(plazas_km) AS plazas_km "
            f"FROM read_parquet('{layout.silver_glob('cnmc_trimestral')}') "
            f"WHERE corredor = '{CORREDOR}' AND operador_id <> 'TOTAL' AND empresa <> '{SIN_GTFS}' "
            f"GROUP BY 1, 2"
        ).df()
    except Exception as exc:
        logger.info("sin Silver de CNMC (%s): se lee de la fuente registrada", str(exc)[:60])
        return None


def _cnmc_de_la_fuente(registro: Path) -> pd.DataFrame:
    """El CSV de CNMC tal cual, para no depender de haber corrido todo el pipeline.

    La calibración tiene que poder lanzarse con solo la oferta descargada; exigir `make 04_silver`
    la haría inútil justo cuando más falta hace, que es al empezar.
    """
    fuente = next(s for s in load_sources(registro) if s.id == "cnmc_indicadores")
    df = pd.read_csv(io.BytesIO(obtener(fuente).content), sep=";", encoding="utf-8-sig", decimal=",")
    df = df[(df["Corredor"] == CORREDOR) & (df["Tipo de producto"] == "LD AV") & (df["Empresa"] != SIN_GTFS)
            & (df["Empresa"] != "Total")]
    df["anio"] = [int(t[:4]) for t in df["Trimestre"]]
    df["trimestre"] = [int(t[-1]) for t in df["Trimestre"]]
    return df.groupby(["anio", "trimestre"]).agg(
        viajeros_km=(COL_CNMC["viajeros_km"], "sum"), plazas_km=(COL_CNMC["plazas_km"], "sum")
    ).reset_index()


def _cnmc(con, layout, registro: Path) -> pd.DataFrame | None:
    """Demanda y capacidad por trimestre del corredor, de los operadores que SÍ están en el NAP."""
    del_silver = _cnmc_del_silver(con, layout)
    if del_silver is not None and not del_silver.empty:
        return del_silver
    try:
        return _cnmc_de_la_fuente(registro)
    except Exception as exc:
        logger.warning("no se ha podido leer CNMC (%s): se omite la comprobación de ocupación", str(exc)[:80])
        return None


def ocupacion_implicada(con, layout, oferta: pd.DataFrame, reglas,
                        registro: Path = Path("config/data_sources.yml")) -> pd.DataFrame | None:
    """Ocupación que implica cada día con las reglas actuales, o None si no se llega a CNMC."""
    cnmc = _cnmc(con, layout, registro)
    if cnmc is None or cnmc.empty:
        return None
    festivos = con.execute(f"SELECT * FROM read_parquet('{layout.silver_glob('festivos')}')").df()
    meteo = con.execute(f"SELECT fecha, ciudad, temperatura_media, precipitacion_mm "
                        f"FROM read_parquet('{layout.silver_glob('meteo')}')").df()
    eventos = pd.read_csv("config/eventos_corredor.csv")[["fecha", "descripcion", "ciudad"]]
    # OJO: el calendario compara contra objetos `date`. Con Timestamp de pandas no casa ninguna
    # fecha y los festivos salen vacíos SIN dar error.
    for d in (festivos, meteo, eventos):
        d["fecha"] = pd.to_datetime(d["fecha"]).dt.date

    trenes = dict(zip(oferta["fecha"], oferta["trenes"]))
    filas = []
    for t in cnmc.itertuples(index=False):
        trimestre = Trimestre(int(t.anio), int(t.trimestre))
        dias = [d for d in trimestre.dias() if d in trenes]
        if not dias:
            continue
        total_trenes = sum(trenes[d] for d in dias)
        plazas_km_tren = t.plazas_km / total_trenes
        idx = indices_base(construir_calendario(trimestre, festivos, eventos, meteo), reglas)
        suma = sum(i.indice for i in idx)
        for i in idx:
            if i.fecha not in trenes:
                continue
            viajeros_km_dia = t.viajeros_km * i.indice / suma
            plazas_km_dia = trenes[i.fecha] * plazas_km_tren
            filas.append({"fecha": i.fecha, "dia_semana": i.fecha.weekday(), "indice": i.indice,
                          "trenes": trenes[i.fecha], "ocupacion": viajeros_km_dia / plazas_km_dia})
    return pd.DataFrame(filas)


def informe(oferta: pd.DataFrame, props: dict[str, float], reglas, ocupacion: pd.DataFrame | None) -> list[str]:
    out = [
        f"CALIBRACIÓN DE LAS REGLAS DE REPARTO CONTRA LA OFERTA REAL · corredor {CORREDOR}",
        f"{len(oferta)} días con cobertura completa, de {oferta['fecha'].min()} a {oferta['fecha'].max()}",
        "",
        "1) COEFICIENTE POR DÍA DE LA SEMANA",
        f"   {'día':5} {'trenes (mediana)':>17} {'propone la oferta':>18} {'regla actual':>13} {'dif':>7}",
    ]
    mediana = oferta.groupby("dia_semana")["trenes"].median()
    for d, dia in enumerate(SEMANA):
        actual = reglas.base_dia_semana[dia]
        out.append(f"   {dia:5} {mediana.get(d, float('nan')):>17.1f} {props.get(dia, float('nan')):>18.2f} "
                   f"{actual:>13.2f} {props.get(dia, 0) - actual:>+7.2f}")

    if ocupacion is None or ocupacion.empty:
        out += ["", "2) OCUPACIÓN IMPLICADA: no se ha podido leer la demanda de CNMC."]
        return out

    out += ["", "2) OCUPACIÓN QUE IMPLICAN LAS REGLAS ACTUALES (viajeros·km / plazas·km)",
            f"   {'día':5} {'ocup. media':>12} {'ocup. máx':>11}  {'veredicto':<26}"]
    g = ocupacion.groupby("dia_semana")["ocupacion"].agg(["mean", "max"])
    for d, r in g.iterrows():
        v = "IMPOSIBLE: pasa del 100%" if r["mean"] > 1 else ("al límite" if r["max"] > 1 else "")
        out.append(f"   {SEMANA[d]:5} {r['mean']:>11.1%} {r['max']:>10.1%}  {v:<26}")
    imposibles = ocupacion[ocupacion["ocupacion"] > 1]
    out += ["", f"   días por encima del 100 %: {len(imposibles)} de {len(ocupacion)} "
                f"({len(imposibles) / len(ocupacion):.1%})"]
    return out


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Calibra las reglas de reparto contra la oferta del NAP.")
    p.add_argument("--reglas", type=Path, default=Path("config/reglas_demanda.yml"))
    p.add_argument("--todos-los-dias", action="store_true",
                   help="incluye también los días sin cobertura completa (no recomendado)")
    args = p.parse_args(argv)

    layout = LakeLayout.from_env(os.environ)
    con = conexion(layout, os.environ)
    reglas = cargar_reglas(args.reglas)
    oferta = oferta_diaria(con, layout, solo_completos=not args.todos_los_dias)
    if oferta.empty:
        logger.error("no hay oferta en %s: lanza antes `make nap-oferta`", layout.silver_glob("oferta_diaria"))
        return 1

    props = coeficientes(oferta)
    print("\n".join(informe(oferta, props, reglas, ocupacion_implicada(con, layout, oferta, reglas))))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
