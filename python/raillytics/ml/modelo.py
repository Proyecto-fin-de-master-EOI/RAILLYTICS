"""Modelo de ocupación diaria del corredor: gradient boosting sobre la tabla de `ml.dataset`.

**Qué predice:** el coeficiente de ocupación de un día (`viajeros·km / plazas·km`) a partir de cómo
es ese día: día de la semana, festivos, tramos (víspera, puente, regreso), eventos y meteo.

**Por qué scikit-learn y no la clase `GBTRegressor` de Spark.** Es el mismo algoritmo —gradient
boosted trees— con otro nombre: en Spark se llama `GBTRegressor` y aquí `GradientBoostingRegressor`.
Se usa esta implementación porque es la que declara el stack del proyecto (ver el README) y porque
el dataset son ~300 días: repartir 300 filas entre ejecutores de Spark cuesta más que calcularlas.

Dentro de scikit-learn se elige `HistGradientBoostingRegressor` y no `GradientBoostingRegressor`
por una razón concreta: **admite valores ausentes de forma nativa**. Hay 16 días sin temperatura
porque AEMET no la publicó, y la ingesta los deja vacíos en vez de inventar un cero. Imputar aquí
una temperatura media contradiría esa decisión; el modelo aprende a tratar el hueco como lo que es.

**La partición es temporal, no aleatoria.** Es una serie en el tiempo: con una partición al azar el
modelo vería días de junio para predecir días de mayo, y el error saldría mejor de lo que será. Se
entrena con los primeros días y se evalúa con los últimos.

**Limitaciones que hay que decir al presentarlo**, y que no son del modelo sino de los datos:

- El objetivo es **mitad sintético**: el denominador (trenes que circularon) es real, pero el
  numerador sale del reparto diario de la DTC, que lo hacen reglas. El modelo aprende de ese
  reparto, así que un error bajo mide sobre todo que ha captado las reglas, no que acierte la
  demanda real. No existe ocupación diaria observada contra la que medir.
- La meteo que entra es la **observada**. A 5 días no se conoce: habría que usar la previsión. Pero
  AEMET no archiva previsiones pasadas, así que entrenar con ellas es imposible hacia atrás. Se usa
  la observada y se deja dicho.

Uso:
    python -m raillytics.ml.modelo
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.inspection import permutation_importance
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score

from raillytics.ingesta.referencia import conexion
from raillytics.ml.dataset import SEMANA, TABLA
from raillytics.utils.lake import LakeLayout

logger = logging.getLogger(__name__)

# Columnas que NO son variables: el objetivo, la fecha y la marca de plausibilidad.
NO_SON_VARIABLES = ("ocupacion", "fecha", "plausible")
# Proporción de días finales que se reservan para evaluar. Con ~270 días utiles son unos 54.
PROPORCION_TEST = 0.2
# Tramos de la validación temporal. Con ~157 días útiles salen unos 26 días por tramo: un solo
# corte da un número que depende de qué semanas tocaron, y con cinco se ve si el resultado se
# sostiene o fue suerte.
TRAMOS_VALIDACION = 5
# Semilla fija: el mismo dataset tiene que dar el mismo modelo, o no se puede comparar nada.
SEMILLA = 42


def cargar(con, layout, solo_plausibles: bool = True) -> pd.DataFrame:
    df = con.execute(f"SELECT * FROM read_parquet('{layout.silver_glob(TABLA)}')").df()
    if df.empty:
        raise ValueError(f"la tabla {TABLA} está vacía: lanza antes `python -m raillytics.ml.dataset`")
    df["fecha"] = pd.to_datetime(df["fecha"])
    df = df.sort_values("fecha", ignore_index=True)
    if solo_plausibles:
        antes = len(df)
        df = df[df["plausible"]].reset_index(drop=True)
        if len(df) != antes:
            logger.info("descartadas %d filas con ocupación imposible (>100%%)", antes - len(df))
    return df


def variables(df: pd.DataFrame) -> pd.DataFrame:
    """Las columnas que entran al modelo, con el día de la semana convertido a columnas 0/1."""
    X = df.drop(columns=[c for c in NO_SON_VARIABLES if c in df.columns])
    # El día de la semana es categórico: «lun» no es menor que «mar», así que no puede ir como número.
    #
    # Las categorías se fijan a los siete días A PROPÓSITO. Si se dejara que pandas las dedujera de
    # cada conjunto, un tramo de evaluación sin ningún domingo no tendría la columna `dia_dom` y no
    # encajaría con el modelo entrenado. Con las categorías fijas, las columnas son siempre las
    # mismas aunque el trozo de datos no contenga todos los días.
    if "dia_semana" in X.columns:
        X["dia_semana"] = pd.Categorical(X["dia_semana"], categories=SEMANA)
        X = pd.get_dummies(X, columns=["dia_semana"], prefix="dia")
    return X.astype(float)


def partir(df: pd.DataFrame, proporcion_test: float = PROPORCION_TEST):
    """Partición TEMPORAL: los primeros días para entrenar, los últimos para evaluar."""
    corte = int(len(df) * (1 - proporcion_test))
    return df.iloc[:corte].copy(), df.iloc[corte:].copy()


def entrenar(train: pd.DataFrame) -> HistGradientBoostingRegressor:
    modelo = HistGradientBoostingRegressor(random_state=SEMILLA)
    modelo.fit(variables(train), train["ocupacion"])
    return modelo


def evaluar(modelo, test: pd.DataFrame) -> dict:
    y = test["ocupacion"]
    pred = modelo.predict(variables(test))
    return {
        "dias_test": int(len(test)),
        "mape": float(mean_absolute_percentage_error(y, pred)),
        "mae_puntos": float(mean_absolute_error(y, pred)) * 100,
        "r2": float(r2_score(y, pred)),
        "desde": str(test["fecha"].min().date()),
        "hasta": str(test["fecha"].max().date()),
    }


def importancia(modelo, test: pd.DataFrame) -> pd.Series:
    """Cuánto empeora el modelo al barajar cada variable.

    Se mide por permutación y sobre los datos de EVALUACIÓN, no sobre los de entrenamiento: así
    dice qué variables ayudan a acertar días que el modelo no ha visto, que es lo que interesa.
    """
    X = variables(test)
    r = permutation_importance(modelo, X, test["ocupacion"], n_repeats=10, random_state=SEMILLA)
    return pd.Series(r.importances_mean, index=X.columns).sort_values(ascending=False)


def validar(df: pd.DataFrame, tramos: int = TRAMOS_VALIDACION) -> pd.DataFrame:
    """Validación temporal en varios tramos: entrena con lo anterior y evalúa con lo siguiente.

    Se compara siempre contra la estrategia más tonta posible —predecir la media de lo que se ha
    visto—, porque un MAPE suelto no dice nada: lo que importa es si el modelo gana a no hacer nada.
    """
    filas = []
    for i, (idx_tr, idx_te) in enumerate(TimeSeriesSplit(n_splits=tramos).split(df), 1):
        tr, te = df.iloc[idx_tr], df.iloc[idx_te]
        modelo = entrenar(tr)
        pred = modelo.predict(variables(te))
        base = np.full(len(te), tr["ocupacion"].mean())
        filas.append({
            "tramo": i,
            "dias_train": len(tr),
            "dias_test": len(te),
            "desde": te["fecha"].min().date(),
            "hasta": te["fecha"].max().date(),
            "mape": mean_absolute_percentage_error(te["ocupacion"], pred),
            "mape_base": mean_absolute_percentage_error(te["ocupacion"], base),
            "r2": r2_score(te["ocupacion"], pred),
        })
    return pd.DataFrame(filas)


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Entrena el modelo de ocupación diaria del corredor.")
    p.add_argument("--incluir-imposibles", action="store_true",
                   help="entrena también con los días de ocupación >100% (no recomendado)")
    p.add_argument("--destino", type=Path, default=Path("resultados/modelo"))
    args = p.parse_args(argv)

    layout = LakeLayout.from_env(os.environ)
    df = cargar(conexion(layout, os.environ), layout, solo_plausibles=not args.incluir_imposibles)
    train, test = partir(df)
    logger.info("entrenamiento: %d días (%s a %s)", len(train),
                train["fecha"].min().date(), train["fecha"].max().date())
    logger.info("evaluación:    %d días (%s a %s)", len(test),
                test["fecha"].min().date(), test["fecha"].max().date())

    modelo = entrenar(train)
    metricas = evaluar(modelo, test)

    logger.info("MAPE %.1f%%  ·  error medio %.1f puntos de ocupación  ·  R2 %.2f",
                metricas["mape"] * 100, metricas["mae_puntos"], metricas["r2"])
    pesos = importancia(modelo, test)
    total = pesos[pesos > 0].sum() or 1.0
    logger.info("variables que más pesan (por permutación, sobre los días de evaluación):")
    for nombre, peso in pesos.head(6).items():
        logger.info("   %-28s %5.1f%%", nombre, max(peso, 0) / total * 100)

    tabla = validar(df)
    logger.info("validación temporal en %d tramos:", len(tabla))
    logger.info("   %-6s %-24s %8s %10s %8s  %s", "tramo", "días evaluados", "MAPE", "MAPE base", "R2", "gana?")
    for r in tabla.itertuples():
        gana = "sí" if r.mape < r.mape_base else "NO"
        logger.info("   %-6d %s a %s  %7.1f%% %9.1f%% %8.2f  %s",
                    r.tramo, r.desde, r.hasta, r.mape * 100, r.mape_base * 100, r.r2, gana)
    logger.info("   media: MAPE %.1f%% (base %.1f%%) · gana en %d de %d tramos",
                tabla["mape"].mean() * 100, tabla["mape_base"].mean() * 100,
                int((tabla["mape"] < tabla["mape_base"]).sum()), len(tabla))
    metricas["validacion_temporal"] = {
        "tramos": len(tabla),
        "mape_medio": float(tabla["mape"].mean()),
        "mape_base_medio": float(tabla["mape_base"].mean()),
        "tramos_en_que_gana": int((tabla["mape"] < tabla["mape_base"]).sum()),
        "r2_medio": float(tabla["r2"].mean()),
    }

    args.destino.mkdir(parents=True, exist_ok=True)
    (args.destino / "metricas.json").write_text(
        json.dumps({**metricas, "variables": int(variables(train).shape[1]),
                    "dias_entrenamiento": int(len(train))}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("métricas en %s", args.destino / "metricas.json")

    if metricas["mape"] < 0.05:
        logger.warning(
            "MAPE por debajo del 5%%: no es un buen resultado, es la señal de que el objetivo lo "
            "generaron las mismas reglas que el modelo está aprendiendo. Hay que decirlo al presentarlo."
        )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
