"""Completitud de Silver: qué parte de las filas no tiene ningún nulo no controlado.

Es el KPI que el plan del Sprint 2 llama «tasa de completitud Silver», con umbral del 98 %:

    completitud = filas sin ningún nulo NO CONTROLADO / filas totales

No confundirlo con la **cobertura de ingesta** (`raillytics.calidad.cobertura_ingesta`), que mide si
llegaron las fuentes. Esta mide si las filas que quedaron en Silver tienen huecos.

**La distinción que lo hace útil: controlado contra hueco.** Un nulo controlado es un estado legítimo
del negocio y contarlo como falta sería ruido:

- `festivo_nombre` es nulo los días que no son festivo. Es la respuesta correcta, no un hueco.
- Un servicio cancelado no tiene hora de llegada.
- CNMC publica los volúmenes en las filas de cada operador y los ingresos en la fila del total, así
  que cada fila tiene nulas las columnas de la otra. Es la forma de su fichero.

Un hueco, en cambio, es una ausencia que preferiríamos no tener:

- AEMET no publicó la temperatura de 100 días. La ingesta los deja vacíos en vez de inventar un cero
  —decisión deliberada— pero siguen siendo días sin dato, y el KPI tiene que verlos.

Si se declarara todo como controlado, el KPI daría el 100 % siempre y no serviría para nada. Por eso
cada entrada de `NULOS_CONTROLADOS` lleva escrito POR QUÉ ese nulo es un estado y no una ausencia.

Uso:
    python -m raillytics.calidad.completitud_silver
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime

import pandas as pd
from dotenv import find_dotenv, load_dotenv

from raillytics.ingesta.referencia import conexion
from raillytics.utils.cargas import ahora, registrar_carga
from raillytics.utils.lake import LakeLayout, ensure_parent_dir

logger = logging.getLogger(__name__)

GOLD_TABLA = "fact_completitud_silver"
UMBRAL = 0.98

# Tablas de Silver que se miden. Las que no existan se omiten con un aviso: no todas las construye el
# mismo `make`, y un clon recién hecho no las tiene todas.
TABLAS = (
    "viajeros_enriquecidos", "puntualidad_enriquecida",
    "cnmc_trimestral", "cnmc_precio_trimestral", "cnmc_precio_mensual",
    "oferta_diaria", "festivos", "meteo", "turismo_mensual", "ml_ocupacion_diaria",
)

# Columnas cuyo nulo es un ESTADO del negocio y no una ausencia de dato. Cada una con su motivo: sin
# el motivo escrito, esta lista se convierte en el sitio donde se esconden los huecos incómodos.
NULOS_CONTROLADOS: dict[str, dict[str, str]] = {
    "viajeros_enriquecidos": {
        "festivo_nombre": "nulo los días que no son festivo",
    },
    "puntualidad_enriquecida": {
        "hora_real": "un servicio cancelado no tiene hora de llegada",
        "retraso_minutos": "sin hora real no hay retraso que calcular",
    },
    "cnmc_trimestral": {
        "viajeros": "CNMC pone los volúmenes en las filas de operador, no en la del total",
        "plazas_ofertadas": "ídem",
        "plazas_km": "ídem",
        "tren_km": "ídem",
        "viajeros_km": "ídem",
        "ingresos_eur": "y los ingresos solo en la fila del total, no en las de operador",
    },
}


def completitud(con, layout: LakeLayout, tablas=TABLAS) -> pd.DataFrame:
    """Una fila por tabla de Silver: filas, filas limpias y qué columnas tienen huecos."""
    filas = []
    for tabla in tablas:
        glob = layout.silver_glob(tabla)
        try:
            columnas = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}')").fetchall()]
        except Exception:  # noqa: BLE001 - una tabla que aún no existe no es un error
            logger.warning("%s: no está en Silver, se omite", tabla)
            continue

        controladas = NULOS_CONTROLADOS.get(tabla, {})
        vigiladas = [c for c in columnas if c not in controladas]
        if not vigiladas:
            continue

        # Una fila está limpia si ninguna de sus columnas vigiladas es nula.
        condicion = " AND ".join(f'"{c}" IS NOT NULL' for c in vigiladas)
        total, limpias = con.execute(
            f"SELECT count(*), count_if({condicion}) FROM read_parquet('{glob}')"
        ).fetchone()
        if not total:
            continue

        # Qué columnas concretas tienen huecos: es lo que se mira cuando el KPI baja.
        sel = ", ".join(f'count(*) - count("{c}") AS "{c}"' for c in vigiladas)
        conteos = con.execute(f"SELECT {sel} FROM read_parquet('{glob}')").fetchone()
        con_huecos = {c: int(n) for c, n in zip(vigiladas, conteos) if n}

        filas.append({
            "tabla": tabla,
            "filas": int(total),
            "filas_limpias": int(limpias),
            "filas_con_hueco": int(total - limpias),
            "completitud": limpias / total,
            "umbral": UMBRAL,
            "cumple_umbral": (limpias / total) >= UMBRAL,
            "columnas_vigiladas": len(vigiladas),
            "columnas_con_nulos_controlados": len(controladas),
            "columnas_con_huecos": ", ".join(f"{c} ({n})" for c, n in sorted(con_huecos.items())) or None,
        })
    return pd.DataFrame(filas)


def publicar(con, layout: LakeLayout, df: pd.DataFrame, inicio: datetime | None = None) -> str:
    """Deja la tabla en Gold. Se sobrescribe entera: es una foto del estado actual de Silver.

    `inicio` es cuándo empezó el paso: lo que cuesta aquí es calcular y no escribir, y el KPI de
    latencia del pipeline suma lo que diga el registro (ver `Ejecucion.tabla`).
    """
    destino = f"{layout.gold_root}/{GOLD_TABLA}/{GOLD_TABLA}.parquet"
    with registrar_carga("calidad_completitud_silver", "gold", layout, con,
                         parametros={"umbral": UMBRAL, "tablas": len(df)}) as ejecucion:
        with ejecucion.tabla(GOLD_TABLA, origen="silver", destino=destino, inicio=inicio) as carga:
            ensure_parent_dir(destino)
            con.register("completitud_silver_frame", df)
            try:
                con.execute(f"COPY completitud_silver_frame TO '{destino}' (FORMAT PARQUET)")
            finally:
                con.unregister("completitud_silver_frame")
            carga.filas = len(df)
    return destino


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    argparse.ArgumentParser(description="Completitud de Silver: filas sin nulos no controlados.").parse_args(argv)
    # Antes de calcular: lo que tarda este paso es el cálculo, y el registro tiene que verlo.
    inicio = ahora()

    layout = LakeLayout.from_env(os.environ)
    con = conexion(layout, os.environ)
    df = completitud(con, layout)
    if df.empty:
        logger.error("no hay ninguna tabla de Silver que medir")
        return 1

    print(f"   {'tabla':26} {'filas':>7} {'limpias':>8} {'completitud':>12}  columnas con huecos")
    for fila in df.itertuples():
        marca = "" if fila.cumple_umbral else "  <-"
        print(f"   {fila.tabla:26} {fila.filas:>7,} {fila.filas_limpias:>8,} "
              f"{fila.completitud:>11.1%}{marca}  {fila.columnas_con_huecos or ''}")
    conjunta = df["filas_limpias"].sum() / df["filas"].sum()
    print()
    print(f"   conjunta: {conjunta:.1%}   umbral del plan {UMBRAL:.0%}   "
          f"tablas por debajo: {int((~df['cumple_umbral']).sum())} de {len(df)}")

    logger.info("%s -> %s", GOLD_TABLA, publicar(con, layout, df, inicio))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
