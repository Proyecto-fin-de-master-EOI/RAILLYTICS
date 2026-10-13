"""Cobertura de ingesta: cuántas fuentes OBLIGATORIAS se descargaron bien cada día.

Es el KPI que el plan llama «cobertura de ingesta», con objetivo del 100 %. No confundirlo con la
«tasa de completitud Silver» del mismo plan, que es otra cosa: aquella mide el porcentaje de registros
sin nulos no controlados DENTRO de Silver, y esta mide si llegaron las fuentes.

Tampoco es el «% de quality gates OK» que ya tenía el cuadro de mandos: aquel mide cuántas reglas se
cumplen y este cuánto dato falta. Se pueden tener todos los gates en verde y faltar tres meses de
datos, porque ningún gate pregunta si están todos los días.

    cobertura = fuentes obligatorias descargadas OK / fuentes obligatorias

**El denominador son las obligatorias, no las 13 del registro.** Tres fuentes (`crtm` y los dos
`renfe_*` de tiempo real) son los ejemplos con los que se montó el framework y hoy no las consume
nadie; contarlas penalizaría una ingesta perfecta. Cuáles son obligatorias se declara en
`config/data_sources.yml` con `obligatoria: true`, que es donde se ve de un vistazo.

Esa definición pilla las dos averías que importan y que son distintas:

  - una fuente que se intentó y **falló** (el servidor devolvió otra cosa, el fichero fue a cuarentena);
  - una fuente del registro que **ni se intentó**, que es la que no se ve en ningún log de error.

La segunda es la peligrosa: una ingesta que descarga doce de trece fuentes sin un solo error parece
perfecta mirando los errores.

**El denominador es el registro de HOY.** Un día antiguo se mide contra las fuentes que hay ahora, así
que si el registro ha crecido, los días anteriores salen por debajo del 100 %. Es deliberado: la
pregunta que contesta el KPI es «¿tengo hoy todo lo que el proyecto dice necesitar?», no «¿funcionó
bien aquel día según lo que entonces se pedía?». Por eso la tabla trae también `fuentes_intentadas`,
para distinguir un fallo real de un registro que ha crecido.

Uso:
    python -m raillytics.calidad.cobertura_ingesta
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import find_dotenv, load_dotenv

from raillytics.ingesta.referencia import conexion
from raillytics.ingesta.sources import load_sources
from raillytics.utils.cargas import registrar_carga
from raillytics.utils.lake import LakeLayout, ensure_parent_dir

logger = logging.getLogger(__name__)

GOLD_TABLA = "fact_cobertura_ingesta"
PROCESO_INGESTA = "bronze_download"
# El plan pide el 100 % de las obligatorias. Se deja en 0.98 como umbral de aviso, para que un
# fallo puntual de una fuente no pinte en rojo un pipeline que por lo demas funciona.
UMBRAL = 0.98


def cobertura(con, layout: LakeLayout, fuentes_registro: set[str]) -> pd.DataFrame:
    """Una fila por día con ingesta: cuántas fuentes obligatorias se descargaron bien.

    Se cuentan fuentes DISTINTAS y no cargas: relanzar la ingesta el mismo día no debe inflar el
    numerador, y con reintentos una fuente puede tener varias filas.
    """
    cargas = f"{layout.gold_root}/_trazabilidad/cargas/*.parquet"
    try:
        df = con.execute(
            f"""
            SELECT CAST(inicio AS DATE)                                   AS fecha,
                   count(DISTINCT tabla)                                  AS fuentes_intentadas,
                   count(DISTINCT CASE WHEN estado = 'ok' THEN tabla END) AS fuentes_ok,
                   count(*)                                               AS cargas
            FROM read_parquet('{cargas}', union_by_name = true)
            WHERE proceso = ?
            GROUP BY 1 ORDER BY 1
            """,
            [PROCESO_INGESTA],
        ).df()
    except Exception as exc:  # noqa: BLE001 - sin trazabilidad no hay nada que medir
        raise ValueError(
            f"no se puede leer la trazabilidad de cargas en {cargas}: lanza antes `make 00_ingest`"
        ) from exc
    if df.empty:
        return df

    # Solo cuentan las fuentes que el registro pide HOY: una fuente retirada no debe sumar.
    del_registro = con.execute(
        f"""
        SELECT CAST(inicio AS DATE) AS fecha, count(DISTINCT tabla) AS ok_del_registro
        FROM read_parquet('{cargas}', union_by_name = true)
        WHERE proceso = ? AND estado = 'ok' AND tabla IN ({",".join("?" * len(fuentes_registro))})
        GROUP BY 1
        """,
        [PROCESO_INGESTA, *sorted(fuentes_registro)],
    ).df()

    df = df.merge(del_registro, on="fecha", how="left")
    df["ok_del_registro"] = df["ok_del_registro"].fillna(0).astype(int)
    df["fuentes_esperadas"] = len(fuentes_registro)
    df["cobertura"] = df["ok_del_registro"] / len(fuentes_registro)
    df["cumple_umbral"] = df["cobertura"] >= UMBRAL
    df["umbral"] = UMBRAL
    return df


def faltantes(con, layout: LakeLayout, fuentes_registro: set[str], fecha) -> list[str]:
    """Qué fuentes del registro no se descargaron bien ese día. Es lo que se mira cuando falla."""
    cargas = f"{layout.gold_root}/_trazabilidad/cargas/*.parquet"
    ok = {
        r[0]
        for r in con.execute(
            f"""
            SELECT DISTINCT tabla FROM read_parquet('{cargas}', union_by_name = true)
            WHERE proceso = ? AND estado = 'ok' AND CAST(inicio AS DATE) = ?
            """,
            [PROCESO_INGESTA, fecha],
        ).fetchall()
    }
    return sorted(fuentes_registro - ok)


def publicar(con, layout: LakeLayout, df: pd.DataFrame) -> str:
    """Deja la tabla en Gold. Se sobrescribe entera: es un recuento, no un histórico incremental."""
    destino = f"{layout.gold_root}/{GOLD_TABLA}/{GOLD_TABLA}.parquet"
    with registrar_carga("calidad_cobertura_ingesta", "gold", layout, con,
                         parametros={"umbral": UMBRAL, "fuentes": int(df["fuentes_esperadas"].max())}) as ejecucion:
        with ejecucion.tabla(GOLD_TABLA, origen="_trazabilidad/cargas", destino=destino) as carga:
            ensure_parent_dir(destino)
            con.register("cobertura_frame", df)
            try:
                con.execute(
                    "COPY (SELECT * REPLACE (CAST(fecha AS DATE) AS fecha) FROM cobertura_frame) "
                    f"TO '{destino}' (FORMAT PARQUET)"
                )
            finally:
                con.unregister("cobertura_frame")
            carga.filas = len(df)
    return destino


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Cobertura de ingesta: fuentes obligatorias descargadas OK.")
    p.add_argument("--registro", type=Path, default=Path("config/data_sources.yml"))
    args = p.parse_args(argv)

    todas = load_sources(args.registro)
    fuentes = {s.id for s in todas if s.obligatoria}
    if not fuentes:
        logger.error("ninguna fuente del registro está marcada como `obligatoria: true`")
        return 1
    layout = LakeLayout.from_env(os.environ)
    con = conexion(layout, os.environ)

    df = cobertura(con, layout, fuentes)
    if df.empty:
        logger.error("no hay ninguna descarga registrada: lanza antes `make 00_ingest`")
        return 1

    logger.info("%d fuentes obligatorias de %d en el registro; umbral de aviso %.0f%%",
                len(fuentes), len(todas), UMBRAL * 100)
    print(f"   {'día':12} {'ok':>4} {'de':>4} {'cobertura':>12}  {'intentadas':>10}")
    for fila in df.itertuples():
        marca = "" if fila.cumple_umbral else "   <- por debajo del umbral"
        print(f"   {str(fila.fecha)[:10]:12} {fila.ok_del_registro:>4} {fila.fuentes_esperadas:>4} "
              f"{fila.cobertura:>11.1%}  {fila.fuentes_intentadas:>10}{marca}")

    ultima = df.iloc[-1]
    if not ultima["cumple_umbral"]:
        print()
        print(f"   fuentes del registro que faltan el {str(ultima['fecha'])[:10]}:")
        for fuente in faltantes(con, layout, fuentes, ultima["fecha"]):
            print(f"     - {fuente}")

    destino = publicar(con, layout, df)
    logger.info("%s: %d días -> %s", GOLD_TABLA, len(df), destino)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
