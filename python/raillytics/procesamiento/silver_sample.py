"""Silver sintético para desarrollar Gold y los dashboards sin datos reales.

Los jobs PySpark de la capa Silver (limpieza + enriquecimiento con AEMET y
festivos) todavía no existen. Este módulo genera, de forma determinista, las
dos tablas Silver con el contrato de columnas que producirán esos jobs, y las
deja donde ellos las dejarán (bucket Silver de MinIO, o SILVER_ROOT local):

  viajeros_enriquecidos    grano (fecha, estación, línea): viajeros del día,
                           meteo de la provincia y festivo nacional.
  puntualidad_enriquecida  grano servicio (tren): hora prevista/real de
                           llegada, retraso, estado, meteo y festivo.

Cuando existan los jobs reales bastará con que escriban estas columnas en los
mismos prefijos; Gold (raillytics.gold.build) y Superset no cambian.

Los datos NO son reales: estaciones y líneas son un subconjunto ilustrativo
con códigos inventados, y demanda y retrasos siguen un modelo simple
(estacionalidad, día de la semana, festivos, lluvia, hora punta) para que los
dashboards tengan algo que contar.

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
    "viajeros", "temperatura_media", "precipitacion_mm", "condicion_meteo", "es_festivo", "festivo_nombre",
)
PUNTUALIDAD_COLUMNS = (
    "fecha", "servicio_id", "linea_id", "estacion_id", "hora_prevista", "hora_real", "retraso_min", "estado",
    "temperatura_media", "precipitacion_mm", "condicion_meteo", "es_festivo",
)

TIPOS_TREN = ("AVE", "Larga Distancia", "Media Distancia", "Cercanías")
CONDICIONES_METEO = ("despejado", "nuboso", "lluvia", "tormenta")
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
class Linea:
    id: str
    nombre: str
    tipo_tren: str
    paradas: tuple[str, ...]  # códigos de estación en orden origen -> destino
    demanda_base: int         # viajeros/día por parada antes de aplicar factores
    servicios_dia: int        # trenes por día (filas de puntualidad)


# Códigos inventados (no son los de Adif); coordenadas aproximadas de la ciudad.
ESTACIONES = (
    Estacion("MADPA", "Madrid Puerta de Atocha", "Madrid", "Comunidad de Madrid", 40.4066, -3.6895, 3.0),
    Estacion("MADCH", "Madrid Chamartín", "Madrid", "Comunidad de Madrid", 40.4722, -3.6825, 2.4),
    Estacion("ALCHE", "Alcalá de Henares", "Madrid", "Comunidad de Madrid", 40.4830, -3.3660, 1.2),
    Estacion("GETAF", "Getafe Centro", "Madrid", "Comunidad de Madrid", 40.3060, -3.7300, 1.0),
    Estacion("BCNSA", "Barcelona Sants", "Barcelona", "Cataluña", 41.3792, 2.1400, 2.6),
    Estacion("MATAR", "Mataró", "Barcelona", "Cataluña", 41.5350, 2.4430, 0.9),
    Estacion("SABAD", "Sabadell Centre", "Barcelona", "Cataluña", 41.5480, 2.1070, 0.9),
    Estacion("GIRON", "Girona", "Girona", "Cataluña", 41.9790, 2.8170, 0.5),
    Estacion("TARRA", "Camp de Tarragona", "Tarragona", "Cataluña", 41.1690, 1.2020, 0.4),
    Estacion("VLCJS", "Valencia Joaquín Sorolla", "Valencia", "Comunidad Valenciana", 39.4600, -0.3830, 1.4),
    Estacion("ALICA", "Alicante", "Alicante", "Comunidad Valenciana", 38.3440, -0.4930, 0.8),
    Estacion("SEVSJ", "Sevilla Santa Justa", "Sevilla", "Andalucía", 37.3920, -5.9750, 1.3),
    Estacion("MALMZ", "Málaga María Zambrano", "Málaga", "Andalucía", 36.7115, -4.4320, 1.0),
    Estacion("CORDO", "Córdoba", "Córdoba", "Andalucía", 37.8880, -4.7900, 0.7),
    Estacion("ZARDE", "Zaragoza Delicias", "Zaragoza", "Aragón", 41.6590, -0.9120, 1.0),
    Estacion("VALLA", "Valladolid", "Valladolid", "Castilla y León", 41.6420, -4.7290, 0.7),
    Estacion("TOLED", "Toledo", "Toledo", "Castilla-La Mancha", 39.8610, -4.0110, 0.4),
    Estacion("ALBAC", "Albacete", "Albacete", "Castilla-La Mancha", 38.9880, -1.8570, 0.4),
    Estacion("BILAB", "Bilbao Abando", "Bizkaia", "País Vasco", 43.2610, -2.9270, 0.8),
    Estacion("SANTA", "Santander", "Cantabria", "Cantabria", 43.4600, -3.8130, 0.5),
    Estacion("OVIED", "Oviedo", "Asturias", "Principado de Asturias", 43.3670, -5.8560, 0.5),
    Estacion("ACORU", "A Coruña", "A Coruña", "Galicia", 43.3530, -8.4090, 0.5),
)

LINEAS = (
    Linea("AVE-MAD-BCN", "AVE Madrid – Barcelona", "AVE", ("MADPA", "ZARDE", "TARRA", "BCNSA"), 3200, 28),
    Linea("AVE-MAD-SEV", "AVE Madrid – Sevilla", "AVE", ("MADPA", "CORDO", "SEVSJ"), 2600, 22),
    Linea("AVE-MAD-VLC", "AVE Madrid – Valencia", "AVE", ("MADPA", "VLCJS"), 2400, 18),
    Linea("AVE-MAD-MAL", "AVE Madrid – Málaga", "AVE", ("MADPA", "CORDO", "MALMZ"), 1800, 14),
    Linea("AVE-MAD-ALC", "AVE Madrid – Alicante", "AVE", ("MADPA", "ALBAC", "ALICA"), 1300, 12),
    Linea("LD-MAD-SAN", "Alvia Madrid – Santander", "Larga Distancia", ("MADCH", "VALLA", "SANTA"), 700, 6),
    Linea("LD-MAD-BIL", "Alvia Madrid – Bilbao", "Larga Distancia", ("MADCH", "VALLA", "BILAB"), 750, 6),
    Linea("LD-MAD-OVI", "Alvia Madrid – Oviedo", "Larga Distancia", ("MADCH", "VALLA", "OVIED"), 650, 5),
    Linea("LD-MAD-COR", "Alvia Madrid – A Coruña", "Larga Distancia", ("MADCH", "ACORU"), 600, 4),
    Linea("MD-MAD-TOL", "Avant Madrid – Toledo", "Media Distancia", ("MADPA", "TOLED"), 900, 16),
    Linea("MD-BCN-GIR", "Regional Barcelona – Girona", "Media Distancia", ("BCNSA", "GIRON"), 800, 20),
    Linea("MD-SEV-COR", "Media Distancia Sevilla – Córdoba", "Media Distancia", ("SEVSJ", "CORDO"), 500, 12),
    Linea("C-MAD-C2", "Cercanías Madrid C-2", "Cercanías", ("ALCHE", "MADPA", "MADCH"), 9000, 70),
    Linea("C-MAD-C4", "Cercanías Madrid C-4", "Cercanías", ("GETAF", "MADPA", "MADCH"), 8000, 70),
    Linea("C-BCN-R1", "Rodalies Barcelona R1", "Cercanías", ("MATAR", "BCNSA"), 7000, 60),
    Linea("C-BCN-R4", "Rodalies Barcelona R4", "Cercanías", ("SABAD", "BCNSA"), 6500, 60),
)

# Factores de demanda por tipo de tren. Índice 0 = lunes ... 6 = domingo.
_FACTOR_DIA_SEMANA = {
    "AVE":             (1.05, 0.85, 0.85, 0.95, 1.25, 0.80, 1.20),
    "Larga Distancia": (1.00, 0.85, 0.85, 0.95, 1.25, 0.85, 1.25),
    "Media Distancia": (1.00, 1.00, 1.00, 1.00, 1.10, 0.70, 0.80),
    "Cercanías":       (1.00, 1.02, 1.02, 1.02, 0.95, 0.45, 0.35),
}
# Índice 0 = enero ... 11 = diciembre.
_FACTOR_MES = {
    "AVE":             (0.85, 0.85, 0.95, 1.05, 1.00, 1.05, 1.20, 1.20, 1.05, 1.00, 0.95, 1.10),
    "Larga Distancia": (0.85, 0.85, 0.95, 1.05, 1.00, 1.05, 1.20, 1.20, 1.05, 1.00, 0.95, 1.10),
    "Media Distancia": (0.95, 0.95, 1.00, 1.00, 1.00, 1.00, 0.95, 0.80, 1.00, 1.00, 1.00, 0.95),
    "Cercanías":       (1.00, 1.00, 1.00, 0.98, 1.00, 0.95, 0.85, 0.60, 1.00, 1.02, 1.02, 0.90),
}
_FACTOR_FESTIVO = {"AVE": 1.15, "Larga Distancia": 1.15, "Media Distancia": 0.80, "Cercanías": 0.40}
# (tipo de tren, condición meteorológica) -> factor sobre la demanda; el resto, 1.0.
_FACTOR_METEO_VIAJEROS = {
    ("Cercanías", "lluvia"): 0.93,
    ("Cercanías", "tormenta"): 0.85,
    ("Media Distancia", "lluvia"): 0.97,
    ("Media Distancia", "tormenta"): 0.92,
}

# Retraso medio en llegada (minutos) por tipo de tren y factores sobre él.
_RETRASO_MEDIO_MIN = {"AVE": 2.5, "Larga Distancia": 6.0, "Media Distancia": 4.0, "Cercanías": 3.0}
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


def generate_silver_sample(start: date, end: date, seed: int = 42) -> SilverSample:
    """Genera las dos tablas Silver para el rango [start, end] (ambos inclusive)."""
    if end < start:
        raise ValueError(f"el rango está invertido: {start} > {end}")
    rng = np.random.default_rng(seed)
    fechas = pd.date_range(start, end, freq="D")
    festivos = {
        dia: nombre
        for year in range(start.year, end.year + 1)
        for dia, nombre in festivos_nacionales(year).items()
    }
    meteo = _meteo(rng, fechas)
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
                "demanda_base": linea.demanda_base * est.peso,
            }
            for linea in LINEAS
            for est in (estaciones[parada] for parada in linea.paradas)
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
        * factor_meteo
        * rng.lognormal(0.0, 0.08, len(df))
    )
    df["viajeros"] = np.rint(df.demanda_base.to_numpy() * factor).astype("int64")
    return df[list(VIAJEROS_COLUMNS)].sort_values(["fecha", "linea_id", "estacion_id"], ignore_index=True)


def _puntualidad(
    rng: np.random.Generator, fechas: pd.DatetimeIndex, meteo: pd.DataFrame, festivos: dict[date, str]
) -> pd.DataFrame:
    estaciones = {e.id: e for e in ESTACIONES}
    dia_semana = fechas.dayofweek.to_numpy()
    partes = []
    for linea in LINEAS:
        servicios_dia = np.full(len(fechas), linea.servicios_dia)
        if linea.tipo_tren == "Cercanías":  # menos oferta en fin de semana
            servicios_dia = np.where(dia_semana >= 5, np.rint(servicios_dia * 0.6), servicios_dia).astype(int)
        fecha = np.repeat(fechas.to_numpy(), servicios_dia)
        n = len(fecha)
        orden = np.concatenate([np.arange(k) for k in servicios_dia])

        if linea.tipo_tren == "Cercanías":  # dos picos: mañana y tarde
            en_pico = rng.random(n) < 0.55
            minutos = np.where(
                en_pico,
                rng.choice([8 * 60 + 15, 18 * 60 + 30], n) + rng.normal(0.0, 55.0, n),
                rng.uniform(5.5 * 60, 23.5 * 60, n),
            )
        else:
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
                        linea.id + "-" + pd.Series(fecha).dt.strftime("%Y%m%d") + "-" + pd.Series(orden).map("{:03d}".format)
                    ),
                    "linea_id": linea.id,
                    "tipo_tren": linea.tipo_tren,
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
        df.tipo_tren.map(_RETRASO_MEDIO_MIN).to_numpy()
        * np.where(hora_punta, np.where(df.tipo_tren == "Cercanías", 1.6, 1.2), 1.0)
        * df.condicion_meteo.map(_FACTOR_METEO_RETRASO).to_numpy()
        * np.where(df.es_festivo, 0.85, 1.0)
        * (1.0 + 0.12 * df.peso_estacion.to_numpy())
    )
    retraso = np.clip(np.rint(rng.gamma(1.2, retraso_medio / 1.2)), 0, 180).astype("int64")
    p_cancelacion = np.where(df.condicion_meteo == "tormenta", 0.025, 0.004)
    cancelado = rng.random(n) < p_cancelacion

    df["retraso_min"] = pd.array(retraso, dtype="Int64")
    df.loc[cancelado, "retraso_min"] = pd.NA
    df["hora_real"] = df.hora_prevista + pd.to_timedelta(retraso, unit="m")
    df.loc[cancelado, "hora_real"] = pd.NaT
    df["estado"] = np.where(cancelado, "cancelado", "realizado")
    return df[list(PUNTUALIDAD_COLUMNS)].sort_values(["fecha", "hora_prevista", "linea_id"], ignore_index=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m raillytics.procesamiento.silver_sample",
        description="Genera un Silver sintético (viajeros y puntualidad) en el lake",
    )
    parser.add_argument("--start", type=date.fromisoformat, help="primer día (AAAA-MM-DD); por defecto, 365 días antes de --end")
    parser.add_argument("--end", type=date.fromisoformat, help="último día (AAAA-MM-DD); por defecto, ayer")
    parser.add_argument("--seed", type=int, default=42, help="semilla: mismas fechas y semilla producen los mismos datos")
    args = parser.parse_args(argv)
    end = args.end or date.today() - timedelta(days=1)
    start = args.start or end - timedelta(days=364)

    layout = LakeLayout.from_env()
    s3 = S3Settings.from_env() if layout.uses_s3 else None
    sample = generate_silver_sample(start, end, args.seed)
    written = write_silver_sample(connect(s3), layout, sample)

    print(f"Silver sintético {start} .. {end} (semilla {args.seed}) -> {layout.silver_root}")
    print(f"  {SILVER_VIAJEROS:<26}{len(sample.viajeros):>10,} filas  {written[SILVER_VIAJEROS]}")
    print(f"  {SILVER_PUNTUALIDAD:<26}{len(sample.puntualidad):>10,} filas  {written[SILVER_PUNTUALIDAD]}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    raise SystemExit(main())
