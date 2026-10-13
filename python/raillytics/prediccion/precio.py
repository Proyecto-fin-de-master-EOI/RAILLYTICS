"""Precio recomendado por día, franja y operador.

El sprint pide dos KPI de precio: **precio recomendado** y **ratio de aceptación**. Éste construye el
primero. El segundo no se puede medir con lo que hay y está argumentado en `docs/hipotesis.md`: mide
el porcentaje de recomendaciones que un revenue manager adopta sin cambiarlas, o sea un proceso humano
de una empresa que no tenemos. Lo que sí deja montado este módulo es el **mecanismo**: cada
recomendación se guarda con sus entradas, la versión de la regla que la produjo y el `run_id` de la
carga, así que el día que hubiera operador el ratio se calcularía sobre esta tabla sin tocar nada.

La regla:

    precio_recomendado = precio_base(operador, trimestre) × (ocupación / referencia) ** (1/|ε|)

acotado al recorrido de precios real de ese operador en el trayecto.

**Qué es dato y qué es hipótesis**, igual que en `raillytics.ml.franja`:

| pieza | de dónde | ¿real? |
| --- | --- | --- |
| precio base | `precio_medio_eur` de silver/cnmc_precio_trimestral | **sí, medido** |
| recorrido al que se acota | mínimo y máximo observados de ese operador | **sí, medido** |
| ocupación prevista | gold/fact_ocupacion_franja | derivada (y con `base_franja` dentro) |
| elasticidad | `precio_recomendado.elasticidad` de config/reglas_demanda.yml | **hipótesis** |

El intento de estimar la elasticidad con los datos de la CNMC, su resultado (−0,299 con error estándar
0,453) y por qué no sirve está en el propio `reglas_demanda.yml`.

Qué dice y qué no dice la recomendación: con la referencia medida la regla es neutra en promedio, así
que lo que señala es **qué días y qué franjas van por encima o por debajo de la media del corredor**.
No afirma que el nivel de precios esté mal, ni predice lo que el operador va a hacer.

Uso:
    python -m raillytics.prediccion.precio
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from dotenv import find_dotenv, load_dotenv

from raillytics.ingesta.referencia import conexion
from raillytics.prediccion.reglas import RUTA_POR_DEFECTO as RUTA_REGLAS
from raillytics.utils.cargas import ahora, registrar_carga
from raillytics.utils.lake import LakeLayout, ensure_parent_dir

logger = logging.getLogger(__name__)

GOLD_TABLA = "fact_precio_recomendado"
CORREDOR = "Madrid-Barcelona"
TRAYECTO = "Madrid-Barcelona"
# `TOTAL` es el agregado del corredor que publica la CNMC, no un operador: sirve de referencia pero no
# se le recomienda un precio.
AGREGADO = "TOTAL"


class Regla:
    """Los parámetros declarados de la regla, con una versión con la que firmar cada recomendación."""

    def __init__(self, elasticidad: float, referencia_ocupacion: float | None) -> None:
        if elasticidad >= 0:
            raise ValueError(f"la elasticidad tiene que ser negativa, no {elasticidad}")
        self.elasticidad = elasticidad
        # None = la media medida de los días plausibles, que se resuelve al construir.
        self.referencia_ocupacion = referencia_ocupacion

    @property
    def exponente(self) -> float:
        """Cuánto se mueve el precio por cada punto de ocupación: 1/|ε|."""
        return 1.0 / abs(self.elasticidad)

    def version(self, referencia: float) -> str:
        """Firma de los parámetros con los que se calculó. Sin ella el ratio de aceptación no sería
        atribuible: no se sabría qué regla se aceptó o se rechazó."""
        canonico = json.dumps(
            {"elasticidad": self.elasticidad, "referencia": round(referencia, 6)},
            sort_keys=True,
        )
        return hashlib.sha256(canonico.encode("utf-8")).hexdigest()[:8]


def cargar_regla(ruta: Path = RUTA_REGLAS) -> Regla:
    datos = yaml.safe_load(ruta.read_text(encoding="utf-8")) or {}
    bloque = datos.get("precio_recomendado")
    if not bloque:
        raise ValueError(f"{ruta} no declara `precio_recomendado`: sin él no hay regla de precio")
    referencia = bloque.get("referencia_ocupacion", "medida")
    if isinstance(referencia, str):
        if referencia != "medida":
            raise ValueError(f"`referencia_ocupacion` admite 'medida' o un número, no {referencia!r}")
        referencia = None
    else:
        referencia = float(referencia)
        if not 0 < referencia <= 1:
            raise ValueError(f"`referencia_ocupacion` fuera de (0, 1]: {referencia}")
    return Regla(float(bloque["elasticidad"]), referencia)


def construir(con, layout: LakeLayout, regla: Regla) -> pd.DataFrame:
    """Una fila por día, franja y operador."""
    franja = con.execute(
        f"""
        SELECT fecha, franja, es_punta, ocupacion, plausible
        FROM read_parquet('{layout.gold_glob("fact_ocupacion_franja")}')
        """
    ).df()
    if franja.empty:
        return franja

    # Precio base y recorrido real, los dos de la misma fuente. El recorrido se calcula sobre TODO el
    # histórico del operador, no sobre el trimestre: es el tope de lo que se ha visto en el corredor.
    precio = con.execute(
        f"""
        WITH p AS (
            SELECT anio, trimestre, operador_id, empresa,
                   CAST(avg(precio_medio_eur) AS DOUBLE) AS precio_base_eur
            FROM read_parquet('{layout.silver_glob("cnmc_precio_trimestral")}')
            WHERE trayecto = ? AND operador_id <> ?
            GROUP BY 1, 2, 3, 4
        ), r AS (
            SELECT operador_id,
                   CAST(min(precio_medio_eur) AS DOUBLE) AS precio_min_eur,
                   CAST(max(precio_medio_eur) AS DOUBLE) AS precio_max_eur
            FROM read_parquet('{layout.silver_glob("cnmc_precio_trimestral")}')
            WHERE trayecto = ? AND operador_id <> ?
            GROUP BY 1
        )
        SELECT p.*, r.precio_min_eur, r.precio_max_eur
        FROM p JOIN r USING (operador_id)
        """,
        [TRAYECTO, AGREGADO, TRAYECTO, AGREGADO],
    ).df()
    if precio.empty:
        return precio

    franja["fecha"] = pd.to_datetime(franja["fecha"])
    franja["anio"] = franja["fecha"].dt.year
    franja["trimestre"] = franja["fecha"].dt.quarter
    df = franja.merge(precio, on=["anio", "trimestre"], how="inner")
    if df.empty:
        logger.error("ningún día cae en un trimestre con precio publicado por la CNMC")
        return df

    # Un operador sin precio publicado en los trimestres que se recomiendan se cae en el join. Pasa de
    # verdad —la CNMC deja de desglosar AVLO a partir de 2026— y conviene decirlo en vez de que
    # desaparezca sin más: no se le inventa un precio base trayéndole el del último trimestre que
    # tuviera, porque entonces la recomendación saldría de un precio que ya no está vigente.
    sin_precio = sorted(set(precio["operador_id"]) - set(df["operador_id"]))
    if sin_precio:
        logger.warning(
            "sin precio publicado en el periodo, quedan fuera: %s", ", ".join(sin_precio)
        )

    # La referencia sale de los días plausibles: los imposibles arrastran el problema del denominador
    # del NAP (da trenes, no plazas) y desplazarían la media hacia arriba.
    buenos = df[df["plausible"]]
    referencia = regla.referencia_ocupacion
    if referencia is None:
        referencia = float(buenos["ocupacion"].mean()) if not buenos.empty else float(df["ocupacion"].mean())
    df["ocupacion_referencia"] = referencia
    df["elasticidad"] = regla.elasticidad
    df["version_regla"] = regla.version(referencia)

    df["factor"] = (df["ocupacion"] / referencia) ** regla.exponente
    df["precio_sin_acotar_eur"] = df["precio_base_eur"] * df["factor"]
    df["precio_recomendado_eur"] = df["precio_sin_acotar_eur"].clip(
        lower=df["precio_min_eur"], upper=df["precio_max_eur"]
    )
    # Que haya que recortar es información, no un detalle de implementación: dice que la regla pide un
    # precio que no se ha visto nunca en el corredor.
    df["recortado"] = df["precio_sin_acotar_eur"] != df["precio_recomendado_eur"]
    df["en_rango_observado"] = ~df["recortado"]
    df["variacion"] = df["precio_recomendado_eur"] / df["precio_base_eur"] - 1

    return df[[
        "fecha", "franja", "es_punta", "operador_id", "empresa", "anio", "trimestre",
        "precio_base_eur", "precio_min_eur", "precio_max_eur",
        "ocupacion", "ocupacion_referencia", "elasticidad",
        "factor", "precio_sin_acotar_eur", "precio_recomendado_eur", "variacion",
        "recortado", "en_rango_observado", "plausible", "version_regla",
    ]].sort_values(["fecha", "franja", "operador_id"], ignore_index=True)


def publicar(con, layout: LakeLayout, df: pd.DataFrame, inicio: datetime | None = None) -> str:
    """Publica la tabla. `inicio` es cuándo empezó el paso: lo que cuesta aquí es calcular,
    no escribir, y el KPI de latencia suma lo registrado (ver `Ejecucion.tabla`)."""
    destino = f"{layout.gold_root}/{GOLD_TABLA}/{GOLD_TABLA}.parquet"
    parametros = {
        "trayecto": TRAYECTO,
        "elasticidad": float(df["elasticidad"].iloc[0]),
        "referencia_ocupacion": round(float(df["ocupacion_referencia"].iloc[0]), 6),
        "version_regla": str(df["version_regla"].iloc[0]),
        "operadores": int(df["operador_id"].nunique()),
    }
    with registrar_carga("precio_recomendado", "gold", layout, con, parametros=parametros) as ejecucion:
        with ejecucion.tabla(GOLD_TABLA, origen="gold/fact_ocupacion_franja", destino=destino, inicio=inicio) as carga:
            ensure_parent_dir(destino)
            con.register("precio_frame", df)
            try:
                con.execute(
                    "COPY (SELECT * REPLACE (CAST(fecha AS DATE) AS fecha) FROM precio_frame) "
                    f"TO '{destino}' (FORMAT PARQUET)"
                )
            finally:
                con.unregister("precio_frame")
            carga.filas = len(df)
    return destino


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Precio recomendado por día, franja y operador.")
    p.add_argument("--reglas", type=Path, default=RUTA_REGLAS)
    args = p.parse_args(argv)
    # Antes de calcular: lo que tarda este paso es el cálculo, y el registro tiene que verlo.
    inicio = ahora()

    layout = LakeLayout.from_env(os.environ)
    con = conexion(layout, os.environ)
    regla = cargar_regla(args.reglas)
    df = construir(con, layout, regla)
    if df.empty:
        logger.error("sin datos: hacen falta `make ml-franja` y la ingesta de precios de la CNMC")
        return 1

    buenos = df[df["plausible"]]
    ref = float(df["ocupacion_referencia"].iloc[0])
    print(f"   elasticidad {regla.elasticidad:+.2f} (exponente {regla.exponente:.2f})   "
          f"referencia de ocupación {ref:.1%}   regla {df['version_regla'].iloc[0]}")
    print()
    print(f"   {'operador':10} {'base':>8} {'recomendado':>12} {'mínimo':>9} {'máximo':>9} "
          f"{'punta':>8} {'valle':>8} {'recortadas':>11}")
    for operador, g in buenos.groupby("operador_id"):
        punta = g[g["es_punta"]]["precio_recomendado_eur"].mean()
        valle = g[~g["es_punta"]]["precio_recomendado_eur"].mean()
        print(f"   {operador:10} {g['precio_base_eur'].mean():>8.2f} "
              f"{g['precio_recomendado_eur'].mean():>12.2f} {g['precio_min_eur'].iloc[0]:>9.2f} "
              f"{g['precio_max_eur'].iloc[0]:>9.2f} {punta:>8.2f} {valle:>8.2f} "
              f"{g['recortado'].mean():>10.1%}")
    print()
    print(f"   en el rango observado: {buenos['en_rango_observado'].mean():.1%}   "
          f"desvío medio sobre el precio real: {buenos['variacion'].abs().mean():.1%}")
    print(f"   recomendaciones: {len(df)}   días: {df['fecha'].nunique()}   "
          f"no plausibles: {int((~df['plausible']).sum())}")

    logger.info("%s -> %s", GOLD_TABLA, publicar(con, layout, df, inicio))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
