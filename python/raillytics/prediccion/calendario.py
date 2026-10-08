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
# Marcas de `tipos_de_dia` y cómo se escriben en el campo «contexto» del calendario del prompt.
ETIQUETAS_CONTEXTO = (
    ("vispera", "víspera de tramo festivo"),
    ("puente", "puente"),
    ("regreso", "regreso de tramo festivo"),
    ("junto_a_evento", "junto a un evento"),
)
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


def tipos_de_dia(calendario: Calendario) -> pd.DataFrame:
    """Los días del calendario con las marcas que dependen de los días de alrededor (columnas booleanas).

    - `puente`: laborable (lun–vie sin festivo) entre dos días libres (sábado, domingo o festivo).
    - Tramo festivo: días libres seguidos, puentes incluidos, con algún festivo o puente; un fin de semana
      normal no lo es.
    - `vispera`: el laborable justo antes de un tramo festivo.
    - `regreso`: el último día de un tramo festivo de dos o más días.
    - `junto_a_evento`: día sin evento propio con evento el día anterior o el siguiente.

    Solo se mira dentro del trimestre: si el vecino que hace falta cae fuera, el día no se marca.
    """
    dias = calendario.dias
    n = len(dias)
    festivo = [bool(f) for f in dias["festivo"]]
    evento = [bool(e) for e in dias["eventos"]]
    libre = [f or d in ("sáb", "dom") for f, d in zip(festivo, dias["dia_semana"])]
    puente = [not libre[i] and 0 < i < n - 1 and libre[i - 1] and libre[i + 1] for i in range(n)]
    libre = [l or p for l, p in zip(libre, puente)]
    vispera = [False] * n
    regreso = [False] * n
    inicio = 0
    while inicio < n:
        if not libre[inicio]:
            inicio += 1
            continue
        fin = inicio
        while fin + 1 < n and libre[fin + 1]:
            fin += 1
        if any(festivo[inicio : fin + 1]) or any(puente[inicio : fin + 1]):
            if inicio > 0:
                vispera[inicio - 1] = True
            if inicio < fin < n - 1:
                regreso[fin] = True
        inicio = fin + 1
    junto_a_evento = [
        not evento[i] and ((i > 0 and evento[i - 1]) or (i < n - 1 and evento[i + 1])) for i in range(n)
    ]
    return dias.assign(puente=puente, vispera=vispera, regreso=regreso, junto_a_evento=junto_a_evento)


def _texto_meteo(ciudad: str, temp: float, prec: float, origen: str) -> str:
    if origen == ORIGEN_SIN_DATOS:
        return f"{ciudad} sin datos"
    partes = [
        "temp ?" if _es_nan(temp) else f"{round(float(temp))}°C",
        "lluvia ?" if _es_nan(prec) else f"{float(prec):.1f}mm",
    ]
    etiqueta = "obs." if origen == ORIGEN_OBSERVADO else "clim."
    return f"{ciudad} {' '.join(partes)} ({etiqueta})"


def lineas_prompt(calendario: Calendario, climatologia: bool = True, contexto: bool = False) -> list[str]:
    """Una línea por día: fecha | día | festivo | eventos | [contexto |] meteo.

    Con `climatologia=False` la meteo solo aparece con dato observado, y sin ninguno se omite el campo: la
    climatología es la misma para todos los días del mes y no distingue un día de otro.
    Con `contexto=True` cada línea dice si el día es víspera, puente, regreso o está junto a un evento (`tipos_de_dia`):
    el LLM no mira las líneas vecinas, así que lo que depende de ellas se le da escrito en la propia línea.
    """
    lineas = []
    for fila in (tipos_de_dia(calendario) if contexto else calendario.dias).itertuples(index=False):
        if fila.eventos:
            eventos = fila.eventos
        else:
            eventos = "ninguno" if calendario.eventos_con_datos else "sin datos"
        meteo = "; ".join(
            _texto_meteo(c, getattr(fila, f"temp_{c}"), getattr(fila, f"prec_{c}"), getattr(fila, f"origen_{c}"))
            for c in CIUDADES
            if climatologia or getattr(fila, f"origen_{c}") == ORIGEN_OBSERVADO
        )
        linea = f"{fila.fecha.isoformat()} | {fila.dia_semana} | festivo: {fila.festivo or 'no'} | eventos: {eventos}"
        if contexto:
            marcas = [etiqueta for columna, etiqueta in ETIQUETAS_CONTEXTO if getattr(fila, columna)]
            linea += f" | contexto: {', '.join(marcas) or 'ninguno'}"
        lineas.append(f"{linea} | meteo: {meteo}" if meteo else linea)
    return lineas
