"""Calendario día a día del trimestre objetivo: festivos, eventos y meteo (observada o climatología).

La meteo de un trimestre futuro no es una previsión (AEMET prevé a ~7 días): para los días sin dato
observado se usa la climatología, el promedio histórico del mismo mes y ciudad calculado con las
filas observadas, y se marca como tal para que el LLM no la confunda con una previsión.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date

import pandas as pd

from raillytics.prediccion.entradas import CIUDADES
from raillytics.prediccion.trimestre import Trimestre

SEMANA = ("lun", "mar", "mié", "jue", "vie", "sáb", "dom")
ORIGEN_OBSERVADO = "observado"
ORIGEN_CLIMATOLOGIA = "climatologia"
ORIGEN_SIN_DATOS = "sin_datos"
SEPARADOR_EVENTOS = " ; "
_ESPACIOS = re.compile(r"\s+")


@dataclass(frozen=True)
class Calendario:
    dias: pd.DataFrame
    eventos_con_datos: bool


def _limpiar(texto: object) -> str:
    """Una sola línea y sin '|' (el separador de campos de cada línea del prompt)."""
    return _ESPACIOS.sub(" ", str(texto).replace("|", "/")).strip()


def _es_nan(valor: object) -> bool:
    return valor is None or (isinstance(valor, float) and math.isnan(valor))


def _eventos_por_dia(eventos: pd.DataFrame, dias: set[date]) -> dict[date, str]:
    por_dia: dict[date, list[str]] = {}
    for fecha, descripcion, ciudad in zip(eventos["fecha"], eventos["descripcion"], eventos["ciudad"]):
        if fecha not in dias:
            continue
        texto = _limpiar(descripcion)
        if ciudad:
            texto += f" ({_limpiar(ciudad)})"
        por_dia.setdefault(fecha, []).append(texto)
    return {fecha: SEPARADOR_EVENTOS.join(sorted(textos)) for fecha, textos in por_dia.items()}


def _climatologia(meteo: pd.DataFrame) -> dict[tuple[str, int], tuple[float, float]]:
    """Media de temperatura y precipitación por (ciudad, mes) con todo el histórico observado."""
    if meteo.empty:
        return {}
    con_mes = meteo.assign(mes=pd.to_datetime(meteo["fecha"]).dt.month)
    medias = con_mes.groupby(["ciudad", "mes"])[["temperatura_media", "precipitacion_mm"]].mean()
    return {
        (ciudad, int(mes)): (temp, prec)
        for (ciudad, mes), temp, prec in zip(medias.index, medias["temperatura_media"], medias["precipitacion_mm"])
    }


def _meteo_dia(ciudad, dia, observada, clima):
    temp, prec = observada.get((dia, ciudad), (math.nan, math.nan))
    if not (_es_nan(temp) and _es_nan(prec)):
        return temp, prec, ORIGEN_OBSERVADO
    temp, prec = clima.get((ciudad, dia.month), (math.nan, math.nan))
    if not (_es_nan(temp) and _es_nan(prec)):
        return temp, prec, ORIGEN_CLIMATOLOGIA
    return math.nan, math.nan, ORIGEN_SIN_DATOS


def construir_calendario(
    trimestre: Trimestre, festivos: pd.DataFrame, eventos: pd.DataFrame, meteo: pd.DataFrame
) -> Calendario:
    dias = trimestre.dias()
    nombres = dict(zip(festivos["fecha"], festivos["nombre"]))
    por_dia = _eventos_por_dia(eventos, set(dias))
    observada = {
        (fecha, ciudad): (temp, prec)
        for fecha, ciudad, temp, prec in zip(
            meteo["fecha"], meteo["ciudad"], meteo["temperatura_media"], meteo["precipitacion_mm"]
        )
    }
    clima = _climatologia(meteo)
    columnas: dict[str, list] = {
        "fecha": dias,
        "dia_semana": [SEMANA[d.weekday()] for d in dias],
        "festivo": [_limpiar(nombres.get(d, "")) for d in dias],
        "eventos": [por_dia.get(d, "") for d in dias],
    }
    for ciudad in CIUDADES:
        filas = [_meteo_dia(ciudad, d, observada, clima) for d in dias]
        columnas[f"temp_{ciudad}"] = [f[0] for f in filas]
        columnas[f"prec_{ciudad}"] = [f[1] for f in filas]
        columnas[f"origen_{ciudad}"] = [f[2] for f in filas]
    return Calendario(pd.DataFrame(columnas), eventos_con_datos=bool(por_dia))


def _texto_meteo(ciudad: str, temp: float, prec: float, origen: str) -> str:
    if origen == ORIGEN_SIN_DATOS:
        return f"{ciudad} sin datos"
    partes = [
        "temp ?" if _es_nan(temp) else f"{round(float(temp))}°C",
        "lluvia ?" if _es_nan(prec) else f"{float(prec):.1f}mm",
    ]
    etiqueta = "obs." if origen == ORIGEN_OBSERVADO else "clim."
    return f"{ciudad} {' '.join(partes)} ({etiqueta})"


def lineas_prompt(calendario: Calendario) -> list[str]:
    """Una línea por día: fecha | día | festivo | eventos | meteo."""
    lineas = []
    for fila in calendario.dias.itertuples(index=False):
        if fila.eventos:
            eventos = fila.eventos
        else:
            eventos = "ninguno" if calendario.eventos_con_datos else "sin datos"
        meteo = "; ".join(
            _texto_meteo(c, getattr(fila, f"temp_{c}"), getattr(fila, f"prec_{c}"), getattr(fila, f"origen_{c}"))
            for c in CIUDADES
        )
        lineas.append(
            f"{fila.fecha.isoformat()} | {fila.dia_semana} | festivo: {fila.festivo or 'no'} "
            f"| eventos: {eventos} | meteo: {meteo}"
        )
    return lineas
