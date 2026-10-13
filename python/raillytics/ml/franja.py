"""Demanda y ocupación por franja horaria del corredor.

El plan pide tres KPI de negocio que necesitan el detalle intradía: ocupación en hora punta, en hora
valle y reducción de plazas vacías. La serie del proyecto era diaria, así que faltaba el reparto.

**Qué es dato y qué es hipótesis**, porque esta tabla mezcla las dos cosas y presentarla sin la
distinción sería engañoso:

| pieza | de dónde | ¿real? |
| --- | --- | --- |
| trenes por franja | `stop_times.txt` del GTFS del NAP, hora de salida del origen | **sí, medido** |
| demanda del día | reparto de la DTC | derivado |
| reparto entre franjas | `base_franja` de config/reglas_demanda.yml | **hipótesis** |

El reparto es:

    demanda_franja  ∝  trenes_franja × base_franja[franja]

y se normaliza para que las franjas de un día sumen la demanda de ese día: el total del trimestre lo
pone CNMC y este reparto no lo toca.

**Por qué el coeficiente es hipótesis y no se pudo medir** está argumentado en el propio
`reglas_demanda.yml`: dentro del día el operador elige libremente a qué hora pone cada tren, así que
la oferta sigue a la demanda de cerca y no queda señal con la que estimar el diferencial de ocupación.
Modelar la demanda como la oferta atenuada —que sí funcionó para el día de la semana— daría aquí la
madrugada como la franja más llena, que es absurdo.

Uso:
    python -m raillytics.ml.franja
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import find_dotenv, load_dotenv

from raillytics.ingesta.nap_oferta import FRANJAS
from raillytics.ingesta.referencia import conexion
from raillytics.prediccion.reglas import RUTA_POR_DEFECTO as RUTA_REGLAS
from raillytics.utils.cargas import registrar_carga
from raillytics.utils.lake import LakeLayout, ensure_parent_dir

logger = logging.getLogger(__name__)

GOLD_TABLA = "fact_ocupacion_franja"
CORREDOR = "Madrid-Barcelona"
PUNTA = {nombre for _, _, nombre, es_punta in FRANJAS if es_punta}


def base_franja(ruta: Path = RUTA_REGLAS) -> dict[str, float]:
    """El perfil por franja declarado en las reglas. Es una hipótesis, no una medida."""
    import yaml

    datos = yaml.safe_load(ruta.read_text(encoding="utf-8")) or {}
    perfil = datos.get("base_franja")
    if not perfil:
        raise ValueError(f"{ruta} no declara `base_franja`: sin él no se puede repartir por franja")
    return {k: float(v) for k, v in perfil.items()}


def construir(con, layout: LakeLayout, perfil: dict[str, float]) -> pd.DataFrame:
    """Una fila por día y franja, con la oferta medida y la demanda y ocupación repartidas."""
    oferta = con.execute(
        f"""
        SELECT fecha, franja, sum(trenes) AS trenes
        FROM read_parquet('{layout.silver_glob("oferta_franja")}')
        WHERE corredor = ? AND cobertura_completa
        GROUP BY 1, 2
        """,
        [CORREDOR],
    ).df()
    if oferta.empty:
        return oferta

    # La ocupación diaria ya trae el numerador y el denominador cuadrados entre sí: se reparte eso y
    # no se recalcula, para que las franjas de un día sumen exactamente su día.
    diaria = con.execute(
        f"""
        SELECT fecha, ocupacion, plausible
        FROM read_parquet('{layout.silver_glob("ml_ocupacion_diaria")}')
        """
    ).df()
    if diaria.empty:
        return diaria

    df = oferta.merge(diaria, on="fecha", how="inner")
    if df.empty:
        return df

    df["factor"] = df["franja"].map(perfil).fillna(1.0)
    # Peso de cada franja dentro de su día: trenes por el factor de la franja.
    df["peso"] = df["trenes"] * df["factor"]
    df["peso_del_dia"] = df.groupby("fecha")["peso"].transform("sum")
    df["cuota_demanda"] = df["peso"] / df["peso_del_dia"]

    # Cuota de la OFERTA del día, que es el reparto que habría si la ocupación fuera plana.
    df["trenes_del_dia"] = df.groupby("fecha")["trenes"].transform("sum")
    df["cuota_oferta"] = df["trenes"] / df["trenes_del_dia"]

    # Ocupación de la franja: la del día escalada por lo que se desvía su cuota de demanda respecto a
    # su cuota de oferta. Si las dos coinciden, la franja va como el día.
    df["ocupacion"] = df["ocupacion"] * df["cuota_demanda"] / df["cuota_oferta"]
    df["es_punta"] = df["franja"].isin(PUNTA)
    # Una ocupación por encima del 100 % es imposible y hereda el problema del denominador: el NAP da
    # trenes y no plazas. Se marca, no se borra.
    df["plausible"] = df["plausible"] & (df["ocupacion"] <= 1.0)
    # Plazas no vendidas, en proporción: es la base del KPI de reducción de plazas vacías.
    df["plazas_vacias"] = (1 - df["ocupacion"]).clip(lower=0)

    return df[[
        "fecha", "franja", "es_punta", "trenes", "cuota_oferta", "cuota_demanda",
        "ocupacion", "plazas_vacias", "plausible",
    ]].sort_values(["fecha", "franja"], ignore_index=True)


def publicar(con, layout: LakeLayout, df: pd.DataFrame) -> str:
    destino = f"{layout.gold_root}/{GOLD_TABLA}/{GOLD_TABLA}.parquet"
    with registrar_carga("ml_ocupacion_franja", "gold", layout, con,
                         parametros={"corredor": CORREDOR, "franjas": int(df["franja"].nunique())}) as ejecucion:
        with ejecucion.tabla(GOLD_TABLA, origen="silver/oferta_franja", destino=destino) as carga:
            ensure_parent_dir(destino)
            con.register("franja_frame", df)
            try:
                con.execute(
                    "COPY (SELECT * REPLACE (CAST(fecha AS DATE) AS fecha) FROM franja_frame) "
                    f"TO '{destino}' (FORMAT PARQUET)"
                )
            finally:
                con.unregister("franja_frame")
            carga.filas = len(df)
    return destino


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Demanda y ocupación por franja horaria.")
    p.add_argument("--reglas", type=Path, default=RUTA_REGLAS)
    args = p.parse_args(argv)

    layout = LakeLayout.from_env(os.environ)
    con = conexion(layout, os.environ)
    df = construir(con, layout, base_franja(args.reglas))
    if df.empty:
        logger.error("sin datos: hacen falta `make nap-oferta` y `make ml-dataset`")
        return 1

    buenos = df[df["plausible"]]
    print(f"   {'franja':14} {'punta':>6} {'trenes/día':>11} {'cuota oferta':>13} {'ocupación':>10}")
    for franja, g in buenos.groupby("franja"):
        print(f"   {franja:14} {'sí' if g['es_punta'].iloc[0] else '':>6} "
              f"{g['trenes'].mean():>11.1f} {g['cuota_oferta'].mean():>12.1%} {g['ocupacion'].mean():>10.1%}")
    punta = buenos[buenos["es_punta"]]["ocupacion"].mean()
    valle = buenos[~buenos["es_punta"]]["ocupacion"].mean()
    print()
    print(f"   hora punta {punta:.1%}   ·   hora valle {valle:.1%}   ·   diferencia {(punta - valle) * 100:+.1f} pp")
    print(f"   días: {df['fecha'].nunique()}   filas no plausibles: {int((~df['plausible']).sum())}")

    logger.info("%s -> %s", GOLD_TABLA, publicar(con, layout, df))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
