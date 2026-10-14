"""Cuota de viajeros de cada operador del corredor, medida con los datos de CNMC.

`raillytics.procesamiento.silver_sample` genera la tabla sintética `viajeros_enriquecidos` con grano
(fecha, estación, línea, operador). De sus tres dimensiones, dos son supuestos inevitables —nadie
publica viajeros por estación ni por línea— pero **la del operador no tenía por qué serlo**: el CSV de
indicadores de CNMC trae columna `Empresa`, así que el reparto entre operadores está medido.

Esto lo calcula y lo imprime. No modifica nada: las cuotas se escriben a mano en `OPERADORES`, con su
procedencia al lado, igual que se hizo con `base_dia_semana` y `base_mes` en las reglas de reparto.

**Lo que sigue siendo supuesto: el reparto entre Renfe y Avlo.** CNMC agrega las dos marcas bajo la
empresa «Renfe Viajeros» porque son la misma compañía, así que la cuota de la empresa es dato y el
reparto interno entre su servicio premium y su low cost no. Se mantiene la proporción que ya tenía el
generador y se dice que es un supuesto.

**Por qué solo los últimos trimestres.** El corredor se liberalizó por tramos: OUIGO entró en 2021 e
Iryo en diciembre de 2022. Promediar desde 2016 mezclaría un monopolio con un mercado de tres y daría
una cuota que no describe ningún momento real.

Uso:
    python -m raillytics.procesamiento.cuotas_operador
"""
from __future__ import annotations

import argparse
import logging
import os

import pandas as pd
from dotenv import find_dotenv, load_dotenv

from raillytics.ingesta.referencia import conexion
from raillytics.utils.lake import LakeLayout

logger = logging.getLogger(__name__)

TABLA = "cnmc_trimestral"
CORREDOR = "Madrid-Barcelona"
# CNMC publica una fila agregada por corredor: hay que sacarla o las cuotas salen a la mitad.
AGREGADO = "TOTAL"
# Dos años: suficiente para promediar la variación trimestral sin llegar a cuando el corredor no
# tenía tres operadores.
TRIMESTRES = 8
# Qué empresa de CNMC corresponde a cada marca del generador. Solo hace falta para las marcas que
# CNMC no separa: Avlo es la low cost de Renfe Viajeros y va dentro de su fila. Iryo y OUIGO usan
# el mismo id en los dos sitios, así que no necesitan entrada.
CNMC_POR_MARCA = {"AVLO": "RENFE"}


def cuotas(con, layout: LakeLayout, corredor: str = CORREDOR, trimestres: int = TRIMESTRES) -> pd.DataFrame:
    """Cuota de viajeros por empresa en los últimos N trimestres: media, mínimo y máximo."""
    por_trimestre = con.execute(
        f"""
        WITH del_corredor AS (
            SELECT anio, trimestre, empresa, operador_id, viajeros
            FROM read_parquet('{layout.silver_glob(TABLA)}')
            WHERE corredor = ? AND operador_id <> ? AND viajeros > 0
        ), ultimos AS (
            SELECT DISTINCT anio, trimestre FROM del_corredor ORDER BY anio DESC, trimestre DESC LIMIT ?
        )
        SELECT d.anio, d.trimestre, d.empresa, d.operador_id,
               d.viajeros / sum(d.viajeros) OVER (PARTITION BY d.anio, d.trimestre) AS cuota
        FROM del_corredor d JOIN ultimos u USING (anio, trimestre)
        """,
        [corredor, AGREGADO, trimestres],
    ).df()
    if por_trimestre.empty:
        return por_trimestre
    return por_trimestre.groupby(["operador_id", "empresa"]).agg(
        cuota=("cuota", "mean"), minimo=("cuota", "min"), maximo=("cuota", "max"),
        trimestres=("cuota", "size"),
    ).reset_index().sort_values("cuota", ascending=False, ignore_index=True)


def informe(resumen: pd.DataFrame, actuales: dict[str, float]) -> list[str]:
    """Las cuotas medidas frente a las que tiene el generador, y el reparto Renfe/Avlo que supone."""
    out = ["CUOTA DE VIAJEROS POR OPERADOR", "",
           f"   fuente: CNMC, indicadores trimestrales, corredor {CORREDOR}",
           f"   ventana: los {int(resumen['trimestres'].max())} trimestres más recientes", "",
           f"   {'empresa':20} {'cuota media':>12} {'mín':>7} {'máx':>7}"]
    for fila in resumen.itertuples():
        out.append(f"   {fila.empresa:20} {fila.cuota:>11.1%} {fila.minimo:>7.1%} {fila.maximo:>7.1%}")

    medido = {fila.operador_id: fila.cuota for fila in resumen.itertuples()}
    propuesto = por_marca(medido, actuales)

    out += ["", "   frente a lo que tiene silver_sample. Se cruza por operador_id y no por el nombre",
            "   de la empresa: el generador usa el nombre societario (ILSA, SNCF Voyageurs España) y",
            "   CNMC la marca comercial (Iryo, OUIGO), así que los textos no coinciden.", "",
            f"   {'operador':10} {'medido':>8} {'generador':>10} {'propone':>9} {'dif':>8}"]
    for operador, nuevo in propuesto.items():
        tiene = actuales.get(operador, 0.0)
        cnmc = medido.get(CNMC_POR_MARCA.get(operador, operador))
        suyo = f"{cnmc:>8.1%}" if cnmc is not None else f"{'agregado':>8}"
        out.append(f"   {operador:10} {suyo} {tiene:>10.1%} {nuevo:>9.3f} {nuevo - tiene:>+8.1%}")

    hermanas = [o for o, c in CNMC_POR_MARCA.items() if c != o]
    if hermanas:
        agrupadas = {CNMC_POR_MARCA[o] for o in hermanas} | set(hermanas)
        total = sum(actuales.get(o, 0.0) for o in agrupadas)
        reparto = ", ".join(f"{o} {actuales.get(o, 0.0) / total:.1%}" for o in sorted(agrupadas)) if total else ""
        out += ["", "   CNMC agrega las marcas de una misma empresa, así que su reparto interno NO es",
                "   dato medido: se conserva la proporción que ya tenía el generador y queda como supuesto.",
                f"   {reparto}"]
    return out


def por_marca(medido: dict[str, float], actuales: dict[str, float]) -> dict[str, float]:
    """Reparte la cuota medida de cada empresa entre sus marcas, conservando su proporción actual.

    CNMC da una cuota por empresa y el generador tiene una fila por marca comercial. Para Iryo y
    OUIGO es lo mismo. Para Renfe Viajeros no: Avlo es su low cost y CNMC no las separa, así que la
    cuota de la empresa se reparte con la proporción que ya tenía el generador, que sigue siendo un
    supuesto y no mejora por este cambio.
    """
    marcas_por_empresa: dict[str, list[str]] = {}
    for marca in actuales:
        marcas_por_empresa.setdefault(CNMC_POR_MARCA.get(marca, marca), []).append(marca)

    propuesto: dict[str, float] = {}
    for empresa, marcas in marcas_por_empresa.items():
        cuota = medido.get(empresa)
        if cuota is None:
            continue
        total = sum(actuales.get(m, 0.0) for m in marcas)
        for marca in marcas:
            proporcion = (actuales.get(marca, 0.0) / total) if total else 1 / len(marcas)
            propuesto[marca] = round(cuota * proporcion, 3)
    return propuesto


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Mide la cuota de cada operador del corredor con datos de CNMC.")
    p.add_argument("--corredor", default=CORREDOR)
    p.add_argument("--trimestres", type=int, default=TRIMESTRES)
    args = p.parse_args(argv)

    from raillytics.procesamiento.silver_sample import OPERADORES

    layout = LakeLayout.from_env(os.environ)
    con = conexion(layout, os.environ)
    resumen = cuotas(con, layout, args.corredor, args.trimestres)
    if resumen.empty:
        logger.error("no hay datos de %s en %s: lanza antes `make 04_silver`", args.corredor,
                     layout.silver_glob(TABLA))
        return 1

    print("\n".join(informe(resumen, {op.id: op.cuota for op in OPERADORES})))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
