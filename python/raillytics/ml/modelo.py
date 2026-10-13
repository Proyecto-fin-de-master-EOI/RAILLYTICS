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
from datetime import datetime, timezone
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
from raillytics.utils.cargas import ahora, registrar_carga
from raillytics.utils.lake import LakeLayout, ensure_parent_dir

logger = logging.getLogger(__name__)

# Columnas que NO son variables: el objetivo, la fecha y la marca de plausibilidad.
NO_SON_VARIABLES = ("ocupacion", "fecha", "plausible")
# Proporción de días finales que se reservan para evaluar. Con ~270 días utiles son unos 54.
PROPORCION_TEST = 0.2
# El unico corredor del proyecto; viaja en la fila de metricas para que la tabla admita mas en el futuro.
CORREDOR = "Madrid-Barcelona"
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


CONFIANZA = 0.90


def entrenar_banda(train: pd.DataFrame, confianza: float = CONFIANZA):
    """Dos modelos cuantílicos que acotan la predicción por abajo y por arriba.

    `HistGradientBoostingRegressor` admite `loss="quantile"`, así que el intervalo no necesita
    suponer nada sobre la distribución del error: cada modelo aprende directamente su cuantil. Son
    los mismos datos y las mismas variables que el modelo central; lo único que cambia es la pérdida.

    La alternativa era la predicción conforme (banda de ancho fijo a partir de los residuos de un
    tramo de calibración). Se eligió la cuantílica porque da una banda que se ESTRECHA o se ENSANCHA
    según el día: en un martes corriente el modelo está más seguro que en una víspera de puente, y
    una banda de ancho fijo no lo recoge.
    """
    margen = (1 - confianza) / 2
    X, y = variables(train), train["ocupacion"]
    bajo = HistGradientBoostingRegressor(loss="quantile", quantile=margen, random_state=SEMILLA).fit(X, y)
    alto = HistGradientBoostingRegressor(loss="quantile", quantile=1 - margen, random_state=SEMILLA).fit(X, y)
    return bajo, alto


def cobertura_banda(df: pd.DataFrame, tramos: int = TRAMOS_VALIDACION,
                    confianza: float = CONFIANZA) -> dict:
    """Qué parte de los días reales cae dentro de su banda, medido con validación temporal.

    Es LA comprobación del intervalo: uno que dice 90 % y cubre el 65 % está mintiendo. Se mide
    igual que el error del modelo —entrenando con el pasado y evaluando en el futuro— porque medirla
    sobre los mismos días con los que se entrenó daría una cobertura inflada.

    Devuelve también la amplitud media: una banda puede cubrir el 90 % simplemente por ser enorme, y
    entonces no dice nada. Las dos cifras hay que leerlas juntas.
    """
    partidor = TimeSeriesSplit(n_splits=tramos)
    orden = df.sort_values("fecha").reset_index(drop=True)
    dentro, total, amplitudes = 0, 0, []
    for i_train, i_test in partidor.split(orden):
        train, test = orden.iloc[i_train], orden.iloc[i_test]
        bajo, alto = entrenar_banda(train, confianza)
        X_test = variables(test).reindex(columns=variables(train).columns, fill_value=0)
        lo, hi = bajo.predict(X_test), alto.predict(X_test)
        real = test["ocupacion"].to_numpy()
        dentro += int(((real >= lo) & (real <= hi)).sum())
        total += len(real)
        amplitudes.extend((hi - lo).tolist())
    return {
        "confianza": confianza,
        "cobertura": dentro / total if total else float("nan"),
        "amplitud_media": float(np.mean(amplitudes)) if amplitudes else float("nan"),
        "dias": total,
    }


# Variables que el plan llama «series temporales»: lo que se sabe del día sin mirar al mundo. El
# modelo BASE solo usa estas; el enriquecido añade festivos, vísperas, eventos y meteo.
VARIABLES_CALENDARIO = ("mes",)
PREFIJOS_CALENDARIO = ("dia_",)


def _solo_calendario(X: pd.DataFrame) -> pd.DataFrame:
    columnas = [c for c in X.columns
                if c in VARIABLES_CALENDARIO or c.startswith(PREFIJOS_CALENDARIO)]
    return X[columnas]


def mejora_por_variables_externas(df: pd.DataFrame, tramos: int = TRAMOS_VALIDACION) -> dict:
    """Cuánto mejora el MAPE al añadir festivos, eventos y meteo al modelo de calendario.

    Es el A/B que pide la hipótesis H3 del plan, con umbral de 1,5 puntos porcentuales. Se mide con
    validación temporal y no con una partición única: con un solo corte la diferencia la decide qué
    semanas tocaron.

    Positivo = las variables externas aportan. Sale ~0, y la razón está en docs/modelo-ocupacion.md:
    la ocupación es demanda entre oferta, y los festivos suben el numerador y bajan el denominador a
    la vez, así que se cancelan.
    """
    partidor = TimeSeriesSplit(n_splits=tramos)
    orden = df.sort_values("fecha").reset_index(drop=True)
    con_todo, solo_calendario = [], []
    for i_train, i_test in partidor.split(orden):
        train, test = orden.iloc[i_train], orden.iloc[i_test]
        X_train, X_test = variables(train), variables(test).reindex(columns=variables(train).columns, fill_value=0)
        y_train, y_test = train["ocupacion"], test["ocupacion"]

        completo = HistGradientBoostingRegressor(random_state=SEMILLA).fit(X_train, y_train)
        con_todo.append(mean_absolute_percentage_error(y_test, completo.predict(X_test)))

        base = HistGradientBoostingRegressor(random_state=SEMILLA).fit(_solo_calendario(X_train), y_train)
        solo_calendario.append(mean_absolute_percentage_error(y_test, base.predict(_solo_calendario(X_test))))

    mape_base = float(np.mean(solo_calendario))
    mape_enriquecido = float(np.mean(con_todo))
    return {
        "mape_solo_calendario": mape_base,
        "mape_enriquecido": mape_enriquecido,
        # En puntos porcentuales, que es como lo expresa el umbral del plan.
        "mejora_pp": (mape_base - mape_enriquecido) * 100,
    }


GOLD_TABLA = "fact_metricas_modelo"


def publicar_metricas(con, layout: LakeLayout, metricas: dict, inicio: datetime | None = None) -> str:
    """Deja las métricas del entrenamiento en Gold, una fila por ejecución.

    Por qué en Gold y no solo en `resultados/modelo/metricas.json`: el plan del Sprint 2 pide MAPE y
    R² como métricas publicadas, y un JSON en el disco de quien entrena no lo ve el cuadro de mandos
    ni queda en la trazabilidad. Una fila por ejecución, además, permite ver si el modelo mejora o
    empeora conforme crece el histórico, que es más útil que el último número suelto.

    El `run_id` da nombre al fichero, así que relanzar el mismo entrenamiento sobrescribe su fila en
    vez de duplicarla.

    `inicio` es cuándo empezó el paso: lo que cuesta aquí es calcular y no escribir, y el KPI de
    latencia del pipeline suma lo que diga el registro (ver `Ejecucion.tabla`).
    """
    vt = metricas.get("validacion_temporal", {})
    # No se llama `ahora` para no tapar el `ahora()` del registro de cargas, que es otra cosa.
    generado = datetime.now(timezone.utc)
    run_id = generado.strftime("%Y%m%dT%H%M%S") + "-ml_ocupacion"
    fila = {
        "run_id": run_id,
        "generado_en": generado.replace(tzinfo=None),
        "corredor": CORREDOR,
        "modelo": "HistGradientBoostingRegressor",
        "objetivo": "coeficiente de ocupacion diaria (viajeros.km / plazas.km)",
        "dias_entrenamiento": metricas.get("dias_entrenamiento"),
        "dias_test": metricas.get("dias_test"),
        "variables": metricas.get("variables"),
        # Partición única: el número que cumple el umbral del plan, y que es frágil.
        "mape": metricas.get("mape"),
        "mae_puntos": metricas.get("mae_puntos"),
        "r2": metricas.get("r2"),
        "desde": metricas.get("desde"),
        "hasta": metricas.get("hasta"),
        # Validación temporal: el número que hay que mirar, con su referencia trivial al lado.
        "vt_tramos": vt.get("tramos"),
        "vt_mape": vt.get("mape_medio"),
        "vt_mape_referencia": vt.get("mape_base_medio"),
        "vt_r2": vt.get("r2_medio"),
        "vt_tramos_en_que_gana": vt.get("tramos_en_que_gana"),
        # Positivo = el modelo bate a predecir la media de lo ya visto. Es LA cifra honesta.
        # Intervalo por regresión cuantílica, con su cobertura MEDIDA al lado: una banda que dice
        # 90 % y cubre 65 % miente, y una que cubre el 90 % por ser enorme tampoco dice nada.
        # H3 del plan: cuanto aporta enriquecer el calendario con festivos, eventos y meteo.
        "mape_solo_calendario": metricas.get("externas", {}).get("mape_solo_calendario"),
        "mejora_variables_externas_pp": metricas.get("externas", {}).get("mejora_pp"),
        "ic_confianza": metricas.get("banda", {}).get("confianza"),
        "ic_cobertura": metricas.get("banda", {}).get("cobertura"),
        "ic_amplitud_media": metricas.get("banda", {}).get("amplitud_media"),
        "mejora_sobre_referencia": (
            None if vt.get("mape_base_medio") is None or vt.get("mape_medio") is None
            else vt["mape_base_medio"] - vt["mape_medio"]
        ),
    }
    destino = f"{layout.gold_root}/{GOLD_TABLA}/{run_id}.parquet"
    with registrar_carga("ml_metricas", "gold", layout, con,
                         parametros={"modelo": fila["modelo"]}) as ejecucion:
        with ejecucion.tabla(GOLD_TABLA, origen="silver/ml_ocupacion_diaria", destino=destino, inicio=inicio) as carga:
            ensure_parent_dir(destino)
            con.register("metricas_modelo", pd.DataFrame([fila]))
            try:
                con.execute(
                    "COPY (SELECT * REPLACE (CAST(generado_en AS TIMESTAMP) AS generado_en, "
                    "CAST(desde AS DATE) AS desde, CAST(hasta AS DATE) AS hasta) "
                    f"FROM metricas_modelo) TO '{destino}' (FORMAT PARQUET)"
                )
            finally:
                con.unregister("metricas_modelo")
            carga.filas = 1
    return destino


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Entrena el modelo de ocupación diaria del corredor.")
    p.add_argument("--incluir-imposibles", action="store_true",
                   help="entrena también con los días de ocupación >100% (no recomendado)")
    p.add_argument("--destino", type=Path, default=Path("resultados/modelo"))
    args = p.parse_args(argv)
    # Antes de calcular: lo que tarda este paso es el cálculo, y el registro tiene que verlo.
    inicio = ahora()

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

    externas = mejora_por_variables_externas(df)
    logger.info("variables externas (H3): calendario solo %.1f%%, enriquecido %.1f%% -> mejora %+.2f pp "
                "(el plan pide >= 1,5)",
                externas["mape_solo_calendario"] * 100, externas["mape_enriquecido"] * 100,
                externas["mejora_pp"])

    banda = cobertura_banda(df)
    logger.info("intervalo del %.0f%% por regresión cuantílica: cubre el %.0f%% de los días, "
                "amplitud media %.1f puntos de ocupación",
                banda["confianza"] * 100, banda["cobertura"] * 100, banda["amplitud_media"] * 100)
    if banda["cobertura"] < banda["confianza"] - 0.05:
        logger.warning(
            "el intervalo dice %.0f%% y cubre el %.0f%%: es optimista. Con %d días de evaluación la "
            "banda se estima con pocos datos; hay que decirlo al presentarla.",
            banda["confianza"] * 100, banda["cobertura"] * 100, banda["dias"],
        )

    completas = {**metricas, "variables": int(variables(train).shape[1]),
                 "dias_entrenamiento": int(len(train)), "banda": banda, "externas": externas}
    destino_gold = publicar_metricas(conexion(layout, os.environ), layout, completas, inicio)
    logger.info("métricas en Gold: %s", destino_gold)

    if metricas["mape"] < 0.05:
        logger.warning(
            "MAPE por debajo del 5%%: no es un buen resultado, es la señal de que el objetivo lo "
            "generaron las mismas reglas que el modelo está aprendiendo. Hay que decirlo al presentarlo."
        )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
