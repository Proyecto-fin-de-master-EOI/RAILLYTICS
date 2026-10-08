"""Tabla de entrenamiento del modelo de ocupación diaria del corredor.

Una fila por día, con el objetivo y las variables. Es el entregable «Silver listo para entrenar»
que pide el Sprint 2.

**El objetivo es la ocupación**, no los viajeros: `viajeros·km / plazas·km`. Y es un objetivo a
medias real, que es lo mejor que se puede hacer hoy:

- el **numerador** sale del reparto diario de la DTC, que es una estimación (el total del trimestre
  es real, de CNMC, pero el reparto entre días lo hacen reglas);
- el **denominador** sale de los trenes que circularon de verdad cada día, según el NAP.

No se usa `viajeros / plazas`: un asiento se vende por tramos, así que ese ratio pasa del 100 %
legítimamente y no serviría ni como objetivo ni como métrica.

**Dos variables que NO entran, y conviene saber por qué.** Las dos producirían fuga de información
(el modelo reconstruiría el objetivo en vez de aprenderlo):

- **la oferta** (`trenes`), porque es el denominador del objetivo;
- **el índice de la DTC**, porque es el numerador.

Con ellas dos el MAPE saldría casi cero y no significaría nada. El patrón semanal de la oferta el
modelo lo acaba captando solo a través del día de la semana.

**Aviso sobre el horizonte.** El plan pide predecir a 5 días. La meteo que entra aquí es la
OBSERVADA, que a 5 días no se conoce: en producción habría que usar la previsión de AEMET. Entrenar
con la observada y predecir con la prevista es un error clásico. Está documentado para decidirlo
antes de entrenar, no después.

Uso:
    python -m raillytics.ml.dataset
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import find_dotenv, load_dotenv

from raillytics.ingesta.referencia import conexion, escribir
from raillytics.prediccion.calendario import construir_calendario, tipos_de_dia
from raillytics.prediccion.calibracion import CORREDOR, oferta_diaria, ocupacion_implicada
from raillytics.prediccion.reglas import cargar_reglas
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.lake import LakeLayout

logger = logging.getLogger(__name__)

TABLA = "ml_ocupacion_diaria"
# Una ocupación por encima del 100 % es físicamente imposible: en esos días el conteo de trenes del
# NAP falla (snapshots con el GTFS incompleto). No se borran, se marcan: quien entrene decide, y la
# cifra de cuántos hay queda a la vista en vez de desaparecer.
OCUPACION_MAXIMA = 1.0
SEMANA = ("lun", "mar", "mié", "jue", "vie", "sáb", "dom")
CIUDADES = ("MAD", "BCN")


def _festivos_y_meteo(con, layout):
    festivos = con.execute(f"SELECT * FROM read_parquet('{layout.silver_glob('festivos')}')").df()
    meteo = con.execute(
        f"SELECT fecha, ciudad, temperatura_media, precipitacion_mm "
        f"FROM read_parquet('{layout.silver_glob('meteo')}')"
    ).df()
    eventos = pd.read_csv("config/eventos_corredor.csv")
    # OJO: el calendario compara contra objetos `date`. Con Timestamp de pandas no casa ninguna
    # fecha y los festivos y eventos salen vacíos SIN dar error.
    for d in (festivos, meteo, eventos):
        d["fecha"] = pd.to_datetime(d["fecha"]).dt.date
    return festivos, meteo, eventos


def _variables_del_calendario(trimestres, festivos, eventos, meteo) -> pd.DataFrame:
    """Las marcas de cada día: día de la semana, festivo, víspera, puente, regreso y evento."""
    filas = []
    for t in trimestres:
        cal = construir_calendario(t, festivos, eventos[["fecha", "descripcion", "ciudad"]], meteo)
        for f in tipos_de_dia(cal).itertuples(index=False):
            filas.append({
                "fecha": f.fecha,
                "dia_semana": f.dia_semana,
                "mes": f.fecha.month,
                "festivo": bool(f.festivo),
                # El ámbito importa en un corredor: el Jueves Santo es festivo en Madrid y no en
                # Cataluña, así que no mueve los viajes igual que uno de los dos extremos.
                "festivo_solo_madrid": "(Madrid)" in (f.festivo or ""),
                "festivo_solo_cataluna": "(Cataluña)" in (f.festivo or ""),
                "vispera": bool(f.vispera),
                "puente": bool(f.puente),
                "regreso": bool(f.regreso),
                "evento": bool(f.eventos),
                "junto_a_evento": bool(f.junto_a_evento),
            })
    return pd.DataFrame(filas)


def _tipo_de_evento(eventos: pd.DataFrame) -> pd.DataFrame:
    """Una columna por tipo de evento (feria, deporte, música, fiesta) y la ciudad."""
    if eventos.empty:
        return pd.DataFrame(columns=["fecha"])
    ancho = pd.get_dummies(eventos[["fecha", "tipo"]], columns=["tipo"], prefix="evento")
    ciudad = pd.get_dummies(eventos[["fecha", "ciudad"]], columns=["ciudad"], prefix="evento_en")
    return ancho.merge(ciudad, on="fecha", how="outer").groupby("fecha", as_index=False).max()


def _meteo_ancha(meteo: pd.DataFrame) -> pd.DataFrame:
    """Una columna por ciudad: la meteo de las dos puntas del corredor en la misma fila."""
    ancha = meteo.pivot_table(index="fecha", columns="ciudad",
                              values=["temperatura_media", "precipitacion_mm"])
    ancha.columns = [f"{v}_{c.lower()}" for v, c in ancha.columns]
    return ancha.reset_index()


def construir(desde: date, hasta: date, registro: Path = Path("config/data_sources.yml")) -> pd.DataFrame:
    """Tabla de entrenamiento: una fila por día con el objetivo y las variables."""
    load_dotenv(find_dotenv(usecwd=True))
    layout = LakeLayout.from_env(os.environ)
    con = conexion(layout, os.environ)
    reglas = cargar_reglas(Path("config/reglas_demanda.yml"))

    oferta = oferta_diaria(con, layout)
    objetivo = ocupacion_implicada(con, layout, oferta, reglas, registro)
    if objetivo is None or objetivo.empty:
        raise ValueError("no se puede calcular la ocupación: falta la oferta o la demanda de CNMC")
    # `indice` y `trenes` son el numerador y el denominador del objetivo: fuera, o el modelo lo
    # reconstruye en vez de aprenderlo.
    objetivo = objetivo[["fecha", "ocupacion"]]
    objetivo = objetivo[(objetivo["fecha"] >= desde) & (objetivo["fecha"] <= hasta)]

    festivos, meteo, eventos = _festivos_y_meteo(con, layout)
    # Trimestre es ordenable (dataclass order=True): no hace falta clave.
    trimestres = sorted({Trimestre.de_fecha(f) for f in objetivo["fecha"]})
    tabla = (
        objetivo
        .merge(_variables_del_calendario(trimestres, festivos, eventos, meteo), on="fecha", how="left")
        .merge(_tipo_de_evento(eventos), on="fecha", how="left")
        .merge(_meteo_ancha(meteo), on="fecha", how="left")
        .sort_values("fecha", ignore_index=True)
    )
    # Los días sin evento no tienen columna de tipo: el `outer` deja NaN, y aquí un NaN es un «no».
    columnas_evento = [c for c in tabla.columns if c.startswith("evento_")]
    tabla[columnas_evento] = tabla[columnas_evento].astype("object").fillna(False).astype(bool)
    tabla["plausible"] = tabla["ocupacion"] <= OCUPACION_MAXIMA
    return tabla


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Construye la tabla de entrenamiento de ocupación diaria.")
    p.add_argument("--desde", type=date.fromisoformat, default=date(2025, 6, 1))
    p.add_argument("--hasta", type=date.fromisoformat, default=date.today())
    args = p.parse_args(argv)

    tabla = construir(args.desde, args.hasta)
    destino = escribir(TABLA, tabla, {"desde": str(args.desde), "hasta": str(args.hasta)},
                       os.environ, __name__)

    logger.info("tabla de entrenamiento: %d filas, %d variables -> %s",
                len(tabla), len(tabla.columns) - 2, destino)
    buenas = tabla[tabla["plausible"]]
    logger.info("  ocupación en las filas plausibles: media %.1f%%, de %.1f%% a %.1f%%",
                buenas["ocupacion"].mean() * 100, buenas["ocupacion"].min() * 100,
                buenas["ocupacion"].max() * 100)
    descartables = len(tabla) - len(buenas)
    if descartables:
        logger.warning("  %d filas con ocupación imposible (>100%%), marcadas plausible=False: "
                       "son días en los que el conteo de trenes del NAP falla", descartables)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
