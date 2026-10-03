"""Quality gates de salida de la predicción.

Se evalúan sobre el DataFrame final ANTES de escribir el CSV. Reutilizan `ResultadoGate` del framework
de calidad del proyecto (raillytics.calidad.registro): un gate bloqueante fallido impide escribir.

El gate de rango se evalúa sobre los índices CRUDOS del LLM, que son los que tienen un contrato ([0.2, 3.0]):
el índice normalizado del CSV (media 1.0) puede salirse de ese rango con salidas perfectamente legítimas
(p. ej. un festivo a 0.2 con una media cruda de 1.07 queda en 0.187), y rechazarlas tiraría minutos de GPU.
"""
from __future__ import annotations

import re
import unicodedata
from collections.abc import Collection, Sequence
from datetime import date

import pandas as pd

from raillytics.calidad.registro import (
    RESULTADO_FALLO,
    RESULTADO_OK,
    SEVERIDAD_AVISO,
    SEVERIDAD_BLOQUEANTE,
    ResultadoGate,
)
from raillytics.prediccion.normalizar import INDICE_MAX, INDICE_MIN

TABLA = "prediccion_demanda_ave_mad_bcn"
MIN_DISPERSION = 0.02  # desviación típica mínima de los índices: por debajo, el LLM devolvió una curva plana
# Fracción máxima de días cuyo motivo nombra otro día de la semana. Por encima, la respuesta va desplazada (cada índice
# corresponde a otro día aunque la fecha sea la buena), como le pasó a mistral-nemo con demanda_v3 en 2025-T3.
MAX_DIA_SEMANA_EQUIVOCADO = 0.05
DIAS_SEMANA = ("lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo")


def _sin_tildes(texto: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", texto.lower()) if unicodedata.category(c) != "Mn")


_SIN_TILDES = tuple(_sin_tildes(d) for d in DIAS_SEMANA)
_DIA_SEMANA = re.compile(r"\b(" + "|".join(_SIN_TILDES) + r")\b")


def dia_semana_del_motivo(motivo: str) -> int | None:
    """El primer día de la semana que nombra el motivo (0 = lunes), sin distinguir tildes ni mayúsculas; None si no nombra ninguno."""
    encontrado = _DIA_SEMANA.search(_sin_tildes(motivo))
    return _SIN_TILDES.index(encontrado.group(1)) if encontrado else None


def _gate(nombre, tipo, severidad, pasa, valor=None, umbral=None, detalle=None) -> ResultadoGate:
    return ResultadoGate(
        tabla=TABLA,
        gate=nombre,
        tipo=tipo,
        severidad=severidad,
        resultado=RESULTADO_OK if pasa else RESULTADO_FALLO,
        valor=None if valor is None else float(valor),
        umbral=umbral,
        detalle=None if pasa else detalle,
    )


def evaluar_gates(
    df: pd.DataFrame,
    dias: Sequence[date],
    total_esperado: int,
    eventos_con_datos: bool,
    indices_crudos: Sequence[float],
    fechas_llm: Collection[str] | None = None,
) -> list[ResultadoGate]:
    """`fechas_llm`: los días cuyo motivo escribió el LLM (en modo eventos, solo los días con evento). El gate del día de la
    semana revisa solo esos y calcula su límite sobre ellos: con pocos días, cualquier motivo equivocado bloquea."""
    esperadas = {d.isoformat() for d in dias}
    fechas = list(df["fecha"])
    faltan = sorted(esperadas - set(fechas))
    sobran = sorted(set(fechas) - esperadas)
    duplicadas = sorted(set(df["fecha"][df["fecha"].duplicated()]))
    cobertura_ok = len(fechas) == len(dias) and not faltan and not sobran and not duplicadas

    vacio = df.empty
    nulos = int(df.isna().sum().sum())
    viajeros_min = 0 if vacio else int(df["viajeros_previstos"].min())
    suma = int(df["viajeros_previstos"].sum())
    indice_min = min(indices_crudos) if len(indices_crudos) else INDICE_MIN
    indice_max = max(indices_crudos) if len(indices_crudos) else INDICE_MAX
    dispersion = 0.0 if vacio else float(df["indice"].std(ddof=0))
    revisados = df if fechas_llm is None else df[df["fecha"].isin(set(fechas_llm))]
    equivocados = [
        (fecha, DIAS_SEMANA[nombrado])
        for fecha, motivo in zip(revisados["fecha"], revisados["motivo"])
        if (nombrado := dia_semana_del_motivo(str(motivo))) is not None
        and nombrado != date.fromisoformat(fecha).weekday()
    ]
    max_equivocados = int(MAX_DIA_SEMANA_EQUIVOCADO * len(revisados))

    return [
        _gate(
            "un_registro_por_dia", "cobertura", SEVERIDAD_BLOQUEANTE, cobertura_ok, len(fechas), str(len(dias)),
            f"faltan {faltan[:5]}, sobran {sobran[:5]}, duplicadas {duplicadas[:5]}",
        ),
        _gate("sin_nulos", "no_nulos", SEVERIDAD_BLOQUEANTE, nulos == 0, nulos, "0", f"{nulos} celdas nulas"),
        _gate(
            "viajeros_no_negativos", "rango", SEVERIDAD_BLOQUEANTE, viajeros_min >= 0, viajeros_min, ">= 0",
            f"hay viajeros previstos negativos (mínimo {viajeros_min})",
        ),
        _gate(
            "indice_en_rango", "rango", SEVERIDAD_BLOQUEANTE,
            indice_min >= INDICE_MIN and indice_max <= INDICE_MAX, indice_max, f"[{INDICE_MIN}, {INDICE_MAX}]",
            f"índice crudo del LLM mínimo {indice_min:.3f} y máximo {indice_max:.3f}: fuera de rango",
        ),
        _gate(
            "suma_igual_total", "suma", SEVERIDAD_BLOQUEANTE, suma == total_esperado, suma, str(total_esperado),
            f"la suma de viajeros previstos es {suma} y el total esperado {total_esperado}",
        ),
        _gate(
            "dia_semana_del_motivo", "consistencia", SEVERIDAD_BLOQUEANTE, len(equivocados) <= max_equivocados,
            len(equivocados), f"<= {max_equivocados}",
            f"{len(equivocados)} motivos nombran otro día de la semana (la respuesta del LLM va desplazada): "
            + ", ".join(f"{f} dice {d}" for f, d in equivocados[:5]),
        ),
        _gate(
            "indice_no_plano", "dispersion", SEVERIDAD_AVISO, dispersion >= MIN_DISPERSION, dispersion,
            f">= {MIN_DISPERSION}", f"desviación típica de los índices {dispersion:.4f}: el LLM devolvió una curva casi plana",
        ),
        _gate(
            "eventos_con_datos", "fuente", SEVERIDAD_AVISO, eventos_con_datos, int(eventos_con_datos), "1",
            "la fuente de eventos no tiene filas dentro del trimestre",
        ),
    ]
