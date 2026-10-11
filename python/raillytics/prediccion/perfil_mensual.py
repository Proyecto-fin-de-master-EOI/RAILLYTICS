"""Calibra el peso de cada mes dentro de su trimestre con el turismo del INE.

El reparto diario de la predicción parte de un total trimestral real (CNMC) y lo distribuye con las
reglas de `config/reglas_demanda.yml`. Esas reglas miran el día de la semana, los festivos y los
puentes, y por tanto **no distinguen un martes de enero de un martes de marzo**: dentro del trimestre
el nivel era plano. Eso es una hipótesis implícita, y es falsa.

Aquí se mide con el único dato mensual del proyecto: los turistas de Madrid a Barcelona del INE
(`raillytics.ingesta.ine_turismo`). Para cada trimestre completo se calcula qué cuota de sus turistas
cae en cada uno de sus tres meses y se promedia por mes de calendario entre los años utilizables. Un
mes neutro vale 1/3 de su trimestre; el coeficiente propuesto es `cuota * 3`.

**Por qué la cuota y no el nivel.** El INE cuenta viajes en todos los medios de transporte y solo los
que implican pernoctación, así que su nivel no es la demanda ferroviaria. Pero la cuota —qué parte del
trimestre cae en cada mes— es una forma, y la forma sí es comparable: lo que se afirma es «marzo
concentra más viajes Madrid-Barcelona que enero», no «hubo tantos viajeros».

**Por qué fuera 2020 y 2021.** La pandemia movió los viajes por razones que no tienen que ver con el
calendario, y promediar esos años metería el confinamiento dentro del perfil de abril. Es la misma
exclusión que se usó al intentar inferir los coeficientes de festivos.

La salida incluye la **desviación típica entre años y el estadístico t** de cada mes contra el 1/3
neutro: un mes cuyo efecto no se distingue de cero no debería moverse de 1.00 solo porque la media de
cinco años salga distinta. No modifica nada, igual que `make calibrar`: imprime lo que propone.

Uso:
    python -m raillytics.prediccion.perfil_mensual
"""
from __future__ import annotations

import argparse
import logging
import math
import os
from pathlib import Path

import pandas as pd
from dotenv import find_dotenv, load_dotenv

from raillytics.ingesta.ine_turismo import TABLA as TABLA_TURISMO
from raillytics.ingesta.referencia import conexion
from raillytics.prediccion.reglas import MESES, cargar_reglas
from raillytics.utils.lake import LakeLayout

logger = logging.getLogger(__name__)

DESTINO = "Barcelona"
# La pandemia no es calendario: 2020 y 2021 quedan fuera del promedio.
ANIOS_FUERA = (2020, 2021)
# Un mes neutro se lleva un tercio de su trimestre.
CUOTA_NEUTRA = 1 / 3
# Por debajo de |t| = 2 el efecto no se distingue del mes neutro con esta muestra (4-5 años por mes).
T_MINIMO = 2.0


def turismo_mensual(con, layout: LakeLayout, destino: str = DESTINO) -> pd.DataFrame:
    """La serie mensual del INE para un destino, con el año y el mes ya separados."""
    df = con.execute(
        f"SELECT fecha, turistas FROM read_parquet('{layout.silver_glob(TABLA_TURISMO)}') "
        "WHERE destino = ? ORDER BY fecha",
        [destino],
    ).df()
    if df.empty:
        return df
    fechas = pd.to_datetime(df["fecha"])
    return df.assign(anio=fechas.dt.year, mes=fechas.dt.month, trimestre=fechas.dt.quarter)


def cuotas_por_mes(serie: pd.DataFrame, anios_fuera: tuple[int, ...] = ANIOS_FUERA) -> pd.DataFrame:
    """Cuota de cada mes de calendario dentro de su trimestre: media, desviación y nº de años.

    Solo entran los trimestres con sus tres meses. Un trimestre a medias no da cuotas, da el reparto
    de los meses que hay, que es otra cosa: con enero y febrero pero sin marzo, enero saldría con el
    50 % y el perfil quedaría al revés.
    """
    completos = serie[~serie["anio"].isin(anios_fuera)]
    filas: list[dict] = []
    for (anio, trimestre), g in completos.groupby(["anio", "trimestre"]):
        if len(g) != 3:
            logger.info("%d-T%d: solo %d meses de 3, se omite", anio, trimestre, len(g))
            continue
        total = g["turistas"].sum()
        if total <= 0:
            continue
        for mes, turistas in zip(g["mes"], g["turistas"]):
            filas.append({"mes": int(mes), "anio": int(anio), "cuota": turistas / total})
    if not filas:
        return pd.DataFrame(columns=["mes", "cuota", "desviacion", "anios", "t"])

    cuotas = pd.DataFrame(filas)
    resumen = cuotas.groupby("mes").agg(
        cuota=("cuota", "mean"), desviacion=("cuota", "std"), anios=("cuota", "size")
    ).reset_index()
    # Error típico de la media: con 4-5 años por mes, una diferencia pequeña frente al tercio neutro
    # puede ser el azar de qué años tocaron.
    error = resumen["desviacion"] / resumen["anios"].pow(0.5)
    diferencia = resumen["cuota"] - CUOTA_NEUTRA
    # Sin dispersión entre años, un mes que se desvía del tercio lo hace siempre igual: es la
    # evidencia más fuerte posible, no la más débil. Dividir por cero daría NaN y el NaN se lee como
    # «no significativo», que es justo lo contrario. No pasa con datos reales, pero la regla importa.
    resumen["t"] = [
        (0.0 if d == 0 else math.copysign(math.inf, d)) if (e == 0 or math.isnan(e)) else d / e
        for d, e in zip(diferencia, error)
    ]
    return resumen


def coeficientes(resumen: pd.DataFrame, t_minimo: float = T_MINIMO) -> dict[str, float]:
    """Traduce las cuotas a los coeficientes de `base_mes`: cuota/(1/3), o 1.00 si no es significativa.

    Los meses cuyo efecto no se distingue del neutro se quedan **exactamente** en 1.000: medir una
    diferencia no da derecho a afirmarla, y dejarlos en 1.000 clavado además mantiene limpia la
    trazabilidad (el motivo del día solo menciona el perfil cuando el perfil hace algo).

    No se renormaliza el trimestre para que sus tres meses promedien 1.000, aunque parezca lo
    prudente: `normalizar_indices` reescala el reparto para que su media sea 1.0 antes de aplicar el
    total de CNMC, así que **un factor común dentro del trimestre se cancela**. Renormalizar aquí solo
    movería los meses neutros a 0.995 y compraría una falsa precisión del medio por ciento.
    """
    coef: dict[str, float] = {}
    for fila in resumen.itertuples():
        significativo = not math.isnan(fila.t) and abs(fila.t) >= t_minimo
        coef[MESES[int(fila.mes) - 1]] = round(fila.cuota / CUOTA_NEUTRA, 3) if significativo else 1.0
    return coef


def indice_estacional(serie: pd.DataFrame, anios_fuera: tuple[int, ...] = ANIOS_FUERA) -> pd.DataFrame:
    """Índice estacional del año completo: cada mes dividido por la media de su año.

    Es una cantidad DISTINTA de `cuotas_por_mes`, y las dos salen de la misma serie porque las usan
    dos consumidores distintos:

    - la cuota DENTRO del trimestre alimenta `base_mes` del reparto de la predicción, que ya tiene el
      total del trimestre puesto por CNMC y solo necesita repartirlo entre sus tres meses;
    - este índice alimenta `_FACTOR_MES` del Silver sintético, que no tiene ningún ancla y necesita la
      estacionalidad del año entero.

    Solo entran los años COMPLETOS: dividir por la media de un año al que le faltan meses sesgaría el
    índice hacia los meses que sí están.
    """
    completos = [int(a) for a, g in serie.groupby("anio") if len(g) == 12 and int(a) not in anios_fuera]
    filas: list[dict] = []
    for anio in completos:
        g = serie[serie["anio"] == anio]
        media = g["turistas"].mean()
        if media <= 0:
            continue
        for mes, turistas in zip(g["mes"], g["turistas"]):
            filas.append({"mes": int(mes), "anio": anio, "indice": turistas / media})
    if not filas:
        return pd.DataFrame(columns=["mes", "indice", "desviacion", "anios", "t"])

    indices = pd.DataFrame(filas)
    resumen = indices.groupby("mes").agg(
        indice=("indice", "mean"), desviacion=("indice", "std"), anios=("indice", "size")
    ).reset_index()
    error = resumen["desviacion"] / resumen["anios"].pow(0.5)
    resumen["t"] = [
        (0.0 if d == 0 else math.copysign(math.inf, d)) if (e == 0 or math.isnan(e)) else d / e
        for d, e in zip(resumen["indice"] - 1.0, error)
    ]
    return resumen


def informe_estacional(resumen: pd.DataFrame) -> list[str]:
    """El índice del año completo, para `_FACTOR_MES` de raillytics.procesamiento.silver_sample."""
    if resumen.empty:
        return ["", "   (no hay ningún año completo fuera de la pandemia: no se puede calcular el",
                "    índice estacional del año)"]
    out = ["", "ÍNDICE ESTACIONAL DEL AÑO COMPLETO (para _FACTOR_MES del Silver sintético)", "",
           f"   años completos usados: {int(resumen['anios'].max())}", "",
           f"   {'mes':5} {'índice':>7} {'desv':>7} {'t vs 1':>7}"]
    for fila in resumen.itertuples():
        out.append(f"   {MESES[int(fila.mes) - 1]:5} {fila.indice:>7.3f} {fila.desviacion:>7.3f} {fila.t:>+7.1f}")
    out += ["", "   Para llevarlo a _FACTOR_MES (son los doce, también los no significativos: esa tabla",
            "   es sintética y busca ser plausible, no inferir nada):", "",
            '    "AVE": (' + ", ".join(f"{fila.indice:.3f}" for fila in resumen.itertuples()) + "),"]
    return out


def informe(resumen: pd.DataFrame, propuestos: dict[str, float], actuales: dict[str, float]) -> list[str]:
    out = ["PESO DE CADA MES DENTRO DE SU TRIMESTRE", "",
           f"   fuente: INE, turistas Madrid -> {DESTINO} (tabla 53267, operación TMOV)",
           f"   fuera del promedio: {', '.join(str(a) for a in ANIOS_FUERA)} (pandemia)", "",
           f"   {'mes':5} {'cuota':>7} {'neutra':>7} {'desv':>7} {'años':>5} {'t':>7} "
           f"{'propone':>8} {'ahora':>7}"]
    for fila in resumen.itertuples():
        nombre = MESES[int(fila.mes) - 1]
        marca = "" if abs(fila.t) >= T_MINIMO else "   <- no se distingue del mes neutro"
        out.append(
            f"   {nombre:5} {fila.cuota:>6.1%} {CUOTA_NEUTRA:>7.1%} {fila.desviacion:>6.1%} "
            f"{fila.anios:>5} {fila.t:>7.1f} {propuestos.get(nombre, 1.0):>8.3f} "
            f"{actuales.get(nombre, 1.0):>7.3f}{marca}"
        )
    out += ["", "   Un mes que no se distingue del neutro se queda en 1.000. No se renormaliza el",
            "   trimestre: normalizar_indices reescala el reparto a media 1.0 antes de aplicar el total",
            "   de CNMC, así que un factor común dentro del trimestre se cancela.", "",
            "   Para llevarlos a config/reglas_demanda.yml:", "", "base_mes:"]
    out += [f"  {nombre}: {propuestos.get(nombre, 1.0):.3f}" for nombre in MESES]
    return out


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Calibra el peso de cada mes con el turismo del INE.")
    p.add_argument("--reglas", type=Path, default=Path("config/reglas_demanda.yml"))
    p.add_argument("--destino", default=DESTINO)
    args = p.parse_args(argv)

    layout = LakeLayout.from_env(os.environ)
    con = conexion(layout, os.environ)
    serie = turismo_mensual(con, layout, args.destino)
    if serie.empty:
        logger.error("no hay turismo en %s: lanza antes `make turismo`", layout.silver_glob(TABLA_TURISMO))
        return 1

    resumen = cuotas_por_mes(serie)
    if resumen.empty:
        logger.error("no hay ningún trimestre completo fuera de %s", ANIOS_FUERA)
        return 1

    actuales = dict(cargar_reglas(args.reglas).base_mes)
    salida = informe(resumen, coeficientes(resumen), actuales)
    # El mismo dato alimenta dos consumidores distintos: la cuota dentro del trimestre va a
    # `base_mes` del reparto, y el índice del año completo a `_FACTOR_MES` del Silver sintético.
    salida += informe_estacional(indice_estacional(serie))
    print("\n".join(salida))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
