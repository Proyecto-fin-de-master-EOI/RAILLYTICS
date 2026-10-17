"""Silver sintético para desarrollar Gold y los dashboards sin datos reales.

Los jobs PySpark de la capa Silver (limpieza + enriquecimiento con AEMET y
festivos) todavía no existen. Este módulo genera, de forma determinista, las
dos tablas Silver con el contrato de columnas que producirán esos jobs, y las
deja donde ellos las dejarán (bucket Silver de MinIO, o SILVER_ROOT local):

  viajeros_enriquecidos    grano (fecha, estación, línea, operador): viajeros
                           del día, meteo de la provincia y festivo nacional.
  puntualidad_enriquecida  grano servicio (tren): operador, hora prevista/real
                           de llegada, retraso, estado, meteo y festivo.

Cuando existan los jobs reales bastará con que escriban estas columnas en los
mismos prefijos; Gold (la app Spark GoldBuilderApp) y Superset no cambian.

Los datos NO son reales y cubren SOLO el corredor AVE Madrid–Barcelona (una
línea, AVE-MAD-BCN, y sus cuatro estaciones: Madrid Puerta de Atocha, Zaragoza
Delicias, Camp de Tarragona y Barcelona Sants) y sus cuatro operadores (Renfe,
Iryo, Ouigo y Avlo), con códigos inventados. Demanda y retrasos siguen un modelo
simple (estacionalidad, día de la semana, festivos, lluvia, hora punta) para que
los dashboards tengan algo que contar.

Qué parte es supuesto y qué parte no, porque importa al presentar la tabla:

  cuota de cada operador   MEDIDA: sale de la columna `Empresa` de CNMC
                           (`make cuotas-operador`). El reparto interno entre
                           Renfe y Avlo sí es supuesto: CNMC las agrega.
  estación y línea         supuesto, y no hay forma de medirlo: nadie publica
                           viajeros por estación en este corredor.
  estacionalidad mensual   MEDIDA: índice del turismo del INE (ver _FACTOR_MES).
  servicios y retrasos     inventados.

Uso:  python -m raillytics.procesamiento.silver_sample [--start AAAA-MM-DD] [--end AAAA-MM-DD] [--seed N]
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date, timedelta

import duckdb
import numpy as np
import pandas as pd

from raillytics.utils.cargas import registrar_carga
from raillytics.utils.lake import LakeLayout, S3Settings, connect, ensure_parent_dir

logger = logging.getLogger(__name__)

SILVER_VIAJEROS = "viajeros_enriquecidos"
SILVER_PUNTUALIDAD = "puntualidad_enriquecida"

# Contrato de columnas de cada tabla Silver (lo que Gold espera encontrar).
VIAJEROS_COLUMNS = (
    "fecha", "estacion_id", "estacion_nombre", "provincia", "comunidad", "latitud", "longitud",
    "linea_id", "linea_nombre", "tipo_tren", "origen", "destino",
    "operador_id", "operador_nombre", "operador_empresa", "operador_segmento",
    "viajeros", "temperatura_media", "precipitacion_mm", "condicion_meteo", "es_festivo", "festivo_nombre",
    # Marca de calidad que pide el plan: True cuando la meteo de ese día y provincia NO viene de
    # AEMET y se ha simulado. Pasa en Zaragoza y Tarragona, que no tienen estación propia dada de
    # alta, y en las fechas que AEMET no publicó.
    "meteo_imputada",
)
PUNTUALIDAD_COLUMNS = (
    "fecha", "servicio_id", "linea_id", "operador_id", "estacion_id", "hora_prevista", "hora_real", "retraso_minutos", "estado_servicio",
    "temperatura_media", "precipitacion_mm", "condicion_meteo", "es_festivo",
)

CORREDOR = "AVE-MAD-BCN"  # la única línea del ejemplo
TIPOS_TREN = ("AVE",)
CONDICIONES_METEO = ("despejado", "nuboso", "lluvia", "tormenta")

# Qué provincia del corredor cubre cada estación de AEMET dada de alta. Zaragoza y Tarragona no
# están: no se les asigna la meteo de otra ciudad —Zaragoza tiene un clima continental muy distinto
# del de Madrid— sino que se quedan con la simulada y marcadas como imputadas.
PROVINCIA_POR_CIUDAD_AEMET = {"MAD": "Madrid", "BCN": "Barcelona"}
# Con la climatología diaria de AEMET (temperatura y precipitación) no se puede saber si estuvo
# nublado, así que «nuboso» no aparece en los días reales. Los umbrales son los mismos que usa la
# simulación para distinguir tormenta de lluvia.
MM_TORMENTA = 15.0
ESTADOS = ("realizado", "cancelado")


@dataclass(frozen=True)
class Estacion:
    id: str
    nombre: str
    provincia: str
    comunidad: str
    latitud: float
    longitud: float
    peso: float  # tamaño relativo: multiplica la demanda y el retraso en llegada


@dataclass(frozen=True)
class Operador:
    id: str
    nombre: str
    empresa: str
    segmento: str
    cuota: float              # parte de la demanda del corredor
    servicios_dia: int        # trenes por día (filas de puntualidad)
    retraso_medio_min: float  # retraso medio en llegada antes de aplicar factores
    p_cancelacion: float      # probabilidad de cancelar un servicio un día normal
    factor_finde: float       # demanda en fin de semana y festivo respecto a un laborable (el low cost sube más)


@dataclass(frozen=True)
class Linea:
    id: str
    nombre: str
    tipo_tren: str
    paradas: tuple[str, ...]  # códigos de estación en orden origen -> destino
    demanda_base: int         # viajeros/día por parada antes de aplicar factores


# Códigos inventados (no son los de Adif); coordenadas aproximadas de la ciudad.
ESTACIONES = (
    Estacion("MADPA", "Madrid Puerta de Atocha", "Madrid", "Comunidad de Madrid", 40.4066, -3.6895, 3.0),
    Estacion("ZARDE", "Zaragoza Delicias", "Zaragoza", "Aragón", 41.6590, -0.9120, 1.0),
    Estacion("TARRA", "Camp de Tarragona", "Tarragona", "Cataluña", 41.1690, 1.2020, 0.4),
    Estacion("BCNSA", "Barcelona Sants", "Barcelona", "Cataluña", 41.3792, 2.1400, 2.6),
)

LINEAS = (
    Linea("AVE-MAD-BCN", "AVE Madrid – Barcelona", "AVE", ("MADPA", "ZARDE", "TARRA", "BCNSA"), 3200),
)

# Operadores del corredor: cuotas que suman 1 y 28 servicios al día en total. Avlo es la marca low cost de Renfe.
#
# CUOTAS CALIBRADAS (2026-10-09). Ya no son inventadas: el CSV de indicadores de CNMC trae columna
# `Empresa`, así que el reparto de viajeros entre operadores está MEDIDO. Media de los ocho
# trimestres más recientes del corredor Madrid-Barcelona (`make cuotas-operador` para recalcularlo):
#
#   empresa           cuota medida   rango observado   antes      ahora
#   Renfe Viajeros        60.3 %      57.1 - 62.3 %     0.55      0.603  (RENFE 0.439 + AVLO 0.164)
#   Iryo                  23.7 %      22.0 - 24.8 %     0.25      0.237
#   OUIGO                 16.0 %      14.9 - 19.0 %     0.20      0.160
#
# Solo los ocho últimos trimestres porque el corredor se liberalizó por tramos (OUIGO entró en 2021,
# Iryo en diciembre de 2022): promediar desde 2016 mezclaría un monopolio con un mercado de tres.
#
# Redondeadas a tres decimales suman 1.000 exacto, que es el invariante que el generador necesita:
# si no sumaran 1, la demanda total del corredor cambiaría sin que nadie lo haya decidido.
#
# LO QUE SIGUE SIENDO SUPUESTO: el reparto entre Renfe y Avlo. CNMC las agrega bajo «Renfe Viajeros»
# porque son la misma compañía, así que se conserva la proporción que ya tenía el generador
# (RENFE 72.7 %, AVLO 27.3 % de la empresa) y eso no mejora con este cambio.
#
# Y lo que no cambia: `servicios_dia`, `retraso_medio_min`, `p_cancelacion` y `factor_finde` siguen
# inventados. El NAP da trenes al día por operador y serviría para calibrar `servicios_dia`, pero no
# cubre a Iryo, así que quedaría medio medido y medio supuesto.
OPERADORES = (
    Operador("RENFE", "Renfe", "Renfe Viajeros", "Alta velocidad", 0.439, 12, 2.5, 0.004, 1.00),   # antes 0.40
    Operador("IRYO", "Iryo", "ILSA (Trenitalia, Air Nostrum)", "Alta velocidad", 0.237, 7, 2.2, 0.003, 1.00),  # antes 0.25
    Operador("OUIGO", "Ouigo", "SNCF Voyageurs España", "Low cost", 0.160, 5, 3.4, 0.008, 1.08),   # antes 0.20
    Operador("AVLO", "Avlo", "Renfe Viajeros", "Low cost", 0.164, 4, 2.8, 0.006, 1.06),            # antes 0.15
)

# Factores de demanda del AVE. Índice 0 = lunes ... 6 = domingo.
_FACTOR_DIA_SEMANA = {
    "AVE": (1.05, 0.85, 0.85, 0.95, 1.25, 0.80, 1.20),
}
# Índice 0 = enero ... 11 = diciembre.
#
# ESTACIONALIDAD CALIBRADA (2026-10-11). Antes era inventada, y estaba equivocada en el mes que más
# importa: suponía 1.20 en agosto —un pico de verano— y el dato dice 0.816, el mes MÁS BAJO del año.
# Un 47 % de error y en dirección contraria.
#
# Sale del turismo interprovincial del INE (silver_turismo_mensual, operación TMOV): para cada año
# completo se divide cada mes por la media de su año y se promedia sobre 2022-2025 (2019 y 2026 están
# incompletos; 2020 y 2021 fuera por la pandemia). `make perfil-mensual` lo recalcula.
#
#   mes    medido    t vs 1    antes      mes    medido    t vs 1    antes
#   ene     0.856     -2.0      0.85      jul     1.071     +2.4      1.20
#   feb     0.886     -1.8      0.85      ago     0.816     -4.5      1.20  <- el error gordo
#   mar     1.036     +0.8      0.95      sep     0.935     -3.2      1.05
#   abr     1.006     +0.4      1.05      oct     1.072     +5.3      1.00
#   may     1.117     +2.8      1.00      nov     1.060     +5.9      0.95
#   jun     1.124     +8.7      1.05      dic     1.020     +0.5      1.10
#
# Que agosto sea el mínimo tiene sentido en un corredor de negocio: en agosto el viaje de trabajo
# desaparece y el madrileño se va a la costa, no a Barcelona. La amplitud apenas cambia (1.38x medida
# frente a 1.41x inventada): el generador tenía bien la MAGNITUD de la estacionalidad y mal la FORMA.
#
# Dos cosas que conviene tener claras:
#   - TMOV es un PROXY: cuenta viajes en todos los medios de transporte y solo los que implican
#     pernoctación. Esta tabla es sintética, así que el objetivo es que sea PLAUSIBLE, no inferir la
#     demanda real; para eso está `base_mes` de config/reglas_demanda.yml, que solo usa la forma
#     dentro del trimestre y se queda en 1.000 en los meses no significativos.
#   - Se usan los doce valores medidos, incluidos los que no se distinguen de 1 (mar, abr, dic), por
#     la misma razón: aquí no se afirma nada sobre el mundo, se genera una muestra.
#
# Coherencia con `base_mes`, que se calculó por otro camino: las dos dicen que agosto es bajo (0.816
# aquí, 0.873 dentro de su trimestre) y julio alto dentro del verano. No se contradicen.
_FACTOR_MES = {
    # antes: (0.85, 0.85, 0.95, 1.05, 1.00, 1.05, 1.20, 1.20, 1.05, 1.00, 0.95, 1.10)
    "AVE": (0.856, 0.886, 1.036, 1.006, 1.117, 1.124, 1.071, 0.816, 0.935, 1.072, 1.060, 1.020),
}
_FACTOR_FESTIVO = {"AVE": 1.15}
# (tipo de tren, condición meteorológica) -> factor sobre la demanda; el resto, 1.0.
_FACTOR_METEO_VIAJEROS: dict[tuple[str, str], float] = {}  # el AVE no varía con el tiempo en este modelo

# Factores sobre el retraso medio de cada operador.
_FACTOR_METEO_RETRASO = {"despejado": 1.0, "nuboso": 1.05, "lluvia": 1.5, "tormenta": 2.5}
_HORAS_PUNTA = (7, 8, 9, 17, 18, 19, 20)


@dataclass(frozen=True)
class SilverSample:
    viajeros: pd.DataFrame
    puntualidad: pd.DataFrame
    # Parámetros con los que se generó (quedan en la trazabilidad de la carga).
    start: date
    end: date
    seed: int


def festivos_nacionales(year: int) -> dict[date, str]:
    """Festivos nacionales de España (los comunes a todas las comunidades)."""
    pascua = _domingo_de_pascua(year)
    return {
        date(year, 1, 1): "Año Nuevo",
        date(year, 1, 6): "Epifanía del Señor",
        pascua - timedelta(days=2): "Viernes Santo",
        date(year, 5, 1): "Fiesta del Trabajo",
        date(year, 8, 15): "Asunción de la Virgen",
        date(year, 10, 12): "Fiesta Nacional de España",
        date(year, 11, 1): "Todos los Santos",
        date(year, 12, 6): "Día de la Constitución",
        date(year, 12, 8): "Inmaculada Concepción",
        date(year, 12, 25): "Natividad del Señor",
    }


def _domingo_de_pascua(year: int) -> date:
    # Algoritmo de Meeus/Jones/Butcher para el calendario gregoriano.
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7  # noqa: E741 (nombre tradicional del algoritmo)
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(year, month, day + 1)


def generate_silver_sample(start: date, end: date, seed: int = 42, *,
                           festivos_reales: dict[date, str] | None = None,
                           meteo_real: pd.DataFrame | None = None) -> SilverSample:
    """Genera las dos tablas Silver para el rango [start, end] (ambos inclusive).

    `festivos_reales` y `meteo_real` son el enriquecimiento de verdad, y es lo que pide el plan
    cuando habla de «cruzar los viajeros con meteorología y festivos». Si no se pasan, se simulan y
    la función sigue siendo pura: es lo que permite probarla sin lago y sin red.

    Lo que aporta pasarlos:
    - los festivos del BOE traen también los de Madrid y Cataluña, no solo los diez nacionales que
      genera la regla; para un corredor que une las dos ciudades eso cambia qué días son festivos;
    - la meteo de AEMET es la observada, frente a un seno con ruido.
    """
    if end < start:
        raise ValueError(f"el rango está invertido: {start} > {end}")
    rng = np.random.default_rng(seed)
    fechas = pd.date_range(start, end, freq="D")
    festivos = festivos_reales if festivos_reales is not None else {
        dia: nombre
        for year in range(start.year, end.year + 1)
        for dia, nombre in festivos_nacionales(year).items()
    }
    meteo = _con_meteo_real(_meteo(rng, fechas), meteo_real)
    return SilverSample(
        viajeros=_viajeros(rng, fechas, meteo, festivos),
        puntualidad=_puntualidad(rng, fechas, meteo, festivos),
        start=start,
        end=end,
        seed=seed,
    )


def write_silver_sample(con: duckdb.DuckDBPyConnection, layout: LakeLayout, sample: SilverSample) -> dict[str, str]:
    """Escribe cada tabla como un único Parquet en su prefijo Silver y devuelve las rutas."""
    # pandas guarda fechas y horas como timestamps de nanosegundos; el contrato
    # es DATE / TIMESTAMP de microsegundos, que es lo que escribiría Spark.
    tables = (
        (SILVER_VIAJEROS, sample.viajeros, "CAST(fecha AS DATE) AS fecha"),
        (
            SILVER_PUNTUALIDAD,
            sample.puntualidad,
            "CAST(fecha AS DATE) AS fecha, "
            "CAST(hora_prevista AS TIMESTAMP) AS hora_prevista, "
            "CAST(hora_real AS TIMESTAMP) AS hora_real",
        ),
    )
    written: dict[str, str] = {}
    parametros = {"start": sample.start, "end": sample.end, "seed": sample.seed}
    with registrar_carga("silver_sample", "silver", layout, con, parametros=parametros) as ejecucion:
        for table, frame, casts in tables:
            dest = layout.silver_file(table)
            with ejecucion.tabla(table, origen=__name__, destino=dest) as carga:
                ensure_parent_dir(dest)
                con.register("silver_sample_frame", frame)
                con.execute(f"COPY (SELECT * REPLACE ({casts}) FROM silver_sample_frame) TO '{dest}' (FORMAT PARQUET)")
                con.unregister("silver_sample_frame")
                carga.filas = len(frame)
            written[table] = dest
            logger.info("%s: %d filas -> %s", table, len(frame), dest)
    return written


def _lookup(table: dict[str, tuple[float, ...]], tipo_tren: np.ndarray, index: np.ndarray) -> np.ndarray:
    """table[tipo_tren][index], vectorizado."""
    matrix = np.array([table[tipo] for tipo in TIPOS_TREN])
    rows = pd.Categorical(tipo_tren, categories=TIPOS_TREN).codes
    return matrix[rows, index]


def _festivo_nombre(fecha: pd.Series, festivos: dict[date, str]) -> pd.Series:
    nombres = fecha.dt.date.map(festivos)
    return nombres.astype(object).where(nombres.notna(), None)


def _meteo(rng: np.random.Generator, fechas: pd.DatetimeIndex) -> pd.DataFrame:
    """Meteo diaria por provincia (lo que AEMET aportaría en el enriquecimiento)."""
    provincias = (
        pd.DataFrame([(e.provincia, e.latitud) for e in ESTACIONES], columns=["provincia", "latitud"])
        .groupby("provincia", as_index=False)
        .latitud.mean()
    )
    grid = pd.merge(pd.DataFrame({"fecha": fechas}), provincias, how="cross")
    n = len(grid)
    lat = grid.latitud.to_numpy()
    doy = grid.fecha.dt.dayofyear.to_numpy()
    mes = grid.fecha.dt.month.to_numpy()

    # Temperatura: más cálida cuanto más al sur, máximo a mediados de julio.
    base = 19.0 - 0.9 * (lat - 36.0)
    amplitud = 9.0 - 0.4 * (lat - 36.0)
    temperatura = base + amplitud * np.sin(2 * np.pi * (doy - 105) / 365.25) + rng.normal(0.0, 2.2, n)

    # Lluvia: el norte es más húmedo; primavera y otoño llueve más, en verano menos.
    p_lluvia = np.where(lat > 42.0, 0.42, 0.18) + np.isin(mes, [3, 4, 5, 10, 11]) * 0.10 - np.isin(mes, [6, 7, 8]) * 0.10
    llueve = rng.random(n) < p_lluvia
    mm = np.where(llueve, rng.gamma(1.6, 4.5, n), 0.0).round(1)
    tormenta = llueve & (rng.random(n) < np.clip((mm - 8.0) / 25.0, 0.0, 0.6))
    nuboso = rng.random(n) < 0.35
    condicion = np.select([tormenta, mm > 0, nuboso], ["tormenta", "lluvia", "nuboso"], default="despejado")

    return pd.DataFrame(
        {
            "fecha": grid.fecha,
            "provincia": grid.provincia,
            "temperatura_media": temperatura.round(1),
            "precipitacion_mm": mm,
            "condicion_meteo": condicion,
        }
    )


def _con_meteo_real(simulada: pd.DataFrame, real: pd.DataFrame | None) -> pd.DataFrame:
    """Sustituye la meteo simulada por la de AEMET donde la haya, y marca el resto como imputada.

    No se rellena una provincia con la estación de otra: Zaragoza no tiene el clima de Madrid. Lo que
    no cubre AEMET se queda simulado y marcado, que es la diferencia entre un hueco declarado y un
    dato inventado que parece medido.
    """
    salida = simulada.copy()
    salida["meteo_imputada"] = True
    if real is None or real.empty:
        return salida

    r = real.copy()
    r["provincia"] = r["ciudad"].map(PROVINCIA_POR_CIUDAD_AEMET)
    r = r.dropna(subset=["provincia", "temperatura_media"])
    r["fecha"] = pd.to_datetime(r["fecha"])
    r = r[["fecha", "provincia", "temperatura_media", "precipitacion_mm"]]

    unido = salida.merge(r, on=["fecha", "provincia"], how="left", suffixes=("", "_real"))
    hay = unido["temperatura_media_real"].notna()
    unido.loc[hay, "temperatura_media"] = unido.loc[hay, "temperatura_media_real"].round(1)
    mm_real = unido.loc[hay, "precipitacion_mm_real"].fillna(0.0)
    unido.loc[hay, "precipitacion_mm"] = mm_real.round(1)
    unido.loc[hay, "condicion_meteo"] = np.select(
        [mm_real >= MM_TORMENTA, mm_real > 0], ["tormenta", "lluvia"], default="despejado"
    )
    unido.loc[hay, "meteo_imputada"] = False
    return unido.drop(columns=["temperatura_media_real", "precipitacion_mm_real"])


def _viajeros(
    rng: np.random.Generator, fechas: pd.DatetimeIndex, meteo: pd.DataFrame, festivos: dict[date, str]
) -> pd.DataFrame:
    estaciones = {e.id: e for e in ESTACIONES}
    pares = pd.DataFrame(
        [
            {
                "linea_id": linea.id,
                "linea_nombre": linea.nombre,
                "tipo_tren": linea.tipo_tren,
                "origen": estaciones[linea.paradas[0]].nombre,
                "destino": estaciones[linea.paradas[-1]].nombre,
                "estacion_id": est.id,
                "estacion_nombre": est.nombre,
                "provincia": est.provincia,
                "comunidad": est.comunidad,
                "latitud": est.latitud,
                "longitud": est.longitud,
                "operador_id": op.id,
                "operador_nombre": op.nombre,
                "operador_empresa": op.empresa,
                "operador_segmento": op.segmento,
                "factor_finde": op.factor_finde,
                "demanda_base": linea.demanda_base * est.peso * op.cuota,
            }
            for linea in LINEAS
            for est in (estaciones[parada] for parada in linea.paradas)
            for op in OPERADORES
        ]
    )
    df = pd.merge(pd.DataFrame({"fecha": fechas}), pares, how="cross")
    df = df.merge(meteo, on=["fecha", "provincia"], how="left")
    df["festivo_nombre"] = _festivo_nombre(df.fecha, festivos)
    df["es_festivo"] = df.festivo_nombre.notna()

    tipo_tren = df.tipo_tren.to_numpy()
    factor_meteo = np.array(
        [_FACTOR_METEO_VIAJEROS.get(clave, 1.0) for clave in zip(df.tipo_tren, df.condicion_meteo)]
    )
    factor = (
        _lookup(_FACTOR_DIA_SEMANA, tipo_tren, df.fecha.dt.dayofweek.to_numpy())
        * _lookup(_FACTOR_MES, tipo_tren, df.fecha.dt.month.to_numpy() - 1)
        * np.where(df.es_festivo, df.tipo_tren.map(_FACTOR_FESTIVO), 1.0)
        * np.where(df.es_festivo | (df.fecha.dt.dayofweek >= 5), df.factor_finde, 1.0)
        * factor_meteo
        * rng.lognormal(0.0, 0.08, len(df))
    )
    df["viajeros"] = np.rint(df.demanda_base.to_numpy() * factor).astype("int64")
    return df[list(VIAJEROS_COLUMNS)].sort_values(["fecha", "linea_id", "estacion_id", "operador_id"], ignore_index=True)


def _puntualidad(
    rng: np.random.Generator, fechas: pd.DatetimeIndex, meteo: pd.DataFrame, festivos: dict[date, str]
) -> pd.DataFrame:
    estaciones = {e.id: e for e in ESTACIONES}
    partes = []
    for linea, op in ((linea, op) for linea in LINEAS for op in OPERADORES):
        servicios_dia = np.full(len(fechas), op.servicios_dia)
        fecha = np.repeat(fechas.to_numpy(), servicios_dia)
        n = len(fecha)
        orden = np.concatenate([np.arange(k) for k in servicios_dia])

        minutos = rng.uniform(7 * 60, 22.5 * 60, n)
        minutos = np.clip(np.rint(minutos), 5 * 60, 23 * 60 + 59).astype("int64")

        # Estación de llegada: cualquier parada menos el origen, ponderada por tamaño.
        llegadas = linea.paradas[1:]
        pesos = np.array([estaciones[parada].peso for parada in llegadas])
        partes.append(
            pd.DataFrame(
                {
                    "fecha": fecha,
                    "servicio_id": (
                        linea.id + "-" + op.id + "-" + pd.Series(fecha).dt.strftime("%Y%m%d") + "-" + pd.Series(orden).map("{:03d}".format)
                    ),
                    "linea_id": linea.id,
                    "operador_id": op.id,
                    "retraso_medio_op": op.retraso_medio_min,
                    "p_cancelacion_op": op.p_cancelacion,
                    "estacion_id": rng.choice(llegadas, size=n, p=pesos / pesos.sum()),
                    "hora_prevista": fecha + minutos.astype("timedelta64[m]"),
                }
            )
        )
    df = pd.concat(partes, ignore_index=True)
    df["provincia"] = df.estacion_id.map({e.id: e.provincia for e in ESTACIONES})
    df["peso_estacion"] = df.estacion_id.map({e.id: e.peso for e in ESTACIONES})
    df = df.merge(meteo, on=["fecha", "provincia"], how="left")
    df["es_festivo"] = _festivo_nombre(df.fecha, festivos).notna()

    n = len(df)
    hora_punta = np.isin(df.hora_prevista.dt.hour.to_numpy(), _HORAS_PUNTA)
    retraso_medio = (
        df.retraso_medio_op.to_numpy()
        * np.where(hora_punta, 1.2, 1.0)
        * df.condicion_meteo.map(_FACTOR_METEO_RETRASO).to_numpy()
        * np.where(df.es_festivo, 0.85, 1.0)
        * (1.0 + 0.12 * df.peso_estacion.to_numpy())
    )
    retraso = np.clip(np.rint(rng.gamma(1.2, retraso_medio / 1.2)), 0, 180).astype("int64")
    p_cancelacion = df.p_cancelacion_op.to_numpy() * np.where(df.condicion_meteo == "tormenta", 6.0, 1.0)
    cancelado = rng.random(n) < p_cancelacion

    df["retraso_minutos"] = pd.array(retraso, dtype="Int64")
    df.loc[cancelado, "retraso_minutos"] = pd.NA
    df["hora_real"] = df.hora_prevista + pd.to_timedelta(retraso, unit="m")
    df.loc[cancelado, "hora_real"] = pd.NaT
    df["estado_servicio"] = np.where(cancelado, "cancelado", "realizado")
    return df[list(PUNTUALIDAD_COLUMNS)].sort_values(["fecha", "hora_prevista", "linea_id", "operador_id"], ignore_index=True)


def _enriquecimiento_real(layout: LakeLayout, s3: "S3Settings | None"):
    """Lee de Silver los festivos del BOE y la meteo de AEMET, si están.

    Son tablas de referencia que construyen `make festivos` y `make meteo`. Si no están —en un clon
    recién hecho, por ejemplo— se avisa y se sigue simulando: el generador tiene que poder correr
    solo, que para eso existe.
    """
    con = connect(s3)
    festivos = meteo = None
    try:
        filas = con.execute(
            f"SELECT fecha, nombre FROM read_parquet('{layout.silver_glob('festivos')}')"
        ).fetchall()
        festivos = {f: n for f, n in filas}
        logger.info("festivos del BOE: %d días", len(festivos))
    except Exception:
        logger.warning("no hay tabla de festivos en Silver: se simulan (lanza `make festivos`)")
    try:
        meteo = con.execute(
            f"SELECT fecha, ciudad, temperatura_media, precipitacion_mm "
            f"FROM read_parquet('{layout.silver_glob('meteo')}')"
        ).df()
        logger.info("meteo de AEMET: %d observaciones de %d ciudades",
                    len(meteo), meteo["ciudad"].nunique())
    except Exception:
        logger.warning("no hay tabla de meteo en Silver: se simula (lanza `make meteo`)")
    return festivos, meteo


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m raillytics.procesamiento.silver_sample",
        description="Genera un Silver sintético (viajeros y puntualidad) en el lake",
    )
    parser.add_argument("--start", type=date.fromisoformat, help="primer día (AAAA-MM-DD); por defecto, 365 días antes de --end")
    parser.add_argument("--end", type=date.fromisoformat, help="último día (AAAA-MM-DD); por defecto, ayer")
    parser.add_argument("--seed", type=int, default=42, help="semilla: mismas fechas y semilla producen los mismos datos")
    parser.add_argument("--sin-enriquecimiento-real", action="store_true",
                        help="simula también los festivos y la meteo en vez de leerlos de Silver "
                             "(para generar la muestra sin lago)")
    args = parser.parse_args(argv)
    end = args.end or date.today() - timedelta(days=1)
    start = args.start or end - timedelta(days=364)

    parser_sin_real = args.sin_enriquecimiento_real

    layout = LakeLayout.from_env()
    s3 = S3Settings.from_env() if layout.uses_s3 else None
    festivos_reales = meteo_real = None
    if not parser_sin_real:
        festivos_reales, meteo_real = _enriquecimiento_real(layout, s3)
    sample = generate_silver_sample(start, end, args.seed,
                                    festivos_reales=festivos_reales, meteo_real=meteo_real)
    written = write_silver_sample(connect(s3), layout, sample)

    print(f"Silver sintético {start} .. {end} (semilla {args.seed}) -> {layout.silver_root}")
    print(f"  {SILVER_VIAJEROS:<26}{len(sample.viajeros):>10,} filas  {written[SILVER_VIAJEROS]}")
    print(f"  {SILVER_PUNTUALIDAD:<26}{len(sample.puntualidad):>10,} filas  {written[SILVER_PUNTUALIDAD]}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    raise SystemExit(main())
