"""Salida de la predicción: DataFrame final, CSV atómico y resumen de coherencia del reparto."""
from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from raillytics.prediccion.calendario import Calendario
from raillytics.prediccion.normalizar import IndiceDia
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.fs import atomic_write_bytes

CORREDOR = "AVE-MAD-BCN"
COLUMNAS_CSV = (
    "fecha", "corredor", "viajeros_previstos", "indice", "motivo", "trimestre",
    "modelo", "version_prompt", "run_id", "generado_en",
)


def construir_dataframe(
    dias: Sequence[date],
    indices: Sequence[IndiceDia],
    viajeros: Sequence[int],
    indices_normalizados: Sequence[float],
    *,
    trimestre: Trimestre,
    modelo: str,
    version_prompt: str,
    run_id: str,
    generado_en: datetime,
) -> pd.DataFrame:
    if [i.fecha for i in indices] != list(dias) or not len(dias) == len(viajeros) == len(indices_normalizados):
        raise ValueError("los índices no coinciden, día a día, con los días del trimestre")
    return pd.DataFrame(
        {
            "fecha": [d.isoformat() for d in dias],
            "corredor": CORREDOR,
            "viajeros_previstos": list(viajeros),
            "indice": list(indices_normalizados),
            "motivo": [i.motivo for i in indices],
            "trimestre": str(trimestre),
            "modelo": modelo,
            "version_prompt": version_prompt,
            "run_id": run_id,
            "generado_en": generado_en.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        columns=list(COLUMNAS_CSV),
    )


def ruta_csv(raiz: Path, trimestre: Trimestre, version_prompt: str, generado_en: datetime) -> Path:
    nombre = f"demanda_diaria_{trimestre}_{version_prompt}_{generado_en:%Y%m%dT%H%M%SZ}.csv"
    return raiz / CORREDOR / str(trimestre) / nombre


def escribir_csv(
    df: pd.DataFrame, raiz: Path, trimestre: Trimestre, version_prompt: str, generado_en: datetime
) -> Path:
    destino = ruta_csv(raiz, trimestre, version_prompt, generado_en)
    if destino.exists():
        raise FileExistsError(f"ya existe {destino}: no se sobrescriben predicciones (reintenta pasado un segundo)")
    destino.parent.mkdir(parents=True, exist_ok=True)
    contenido = df.to_csv(index=False, float_format="%.6f", lineterminator="\n").encode("utf-8")
    return atomic_write_bytes(destino, contenido)


def resumen_coherencia(df: pd.DataFrame, calendario: Calendario) -> dict[str, float | None]:
    """Índice medio por tipo de día. No hay verdad externa: sirve para comparar versiones del prompt."""
    cal = calendario.dias.assign(fecha=calendario.dias["fecha"].map(lambda d: d.isoformat()))
    m = df[["fecha", "indice"]].merge(cal[["fecha", "dia_semana", "festivo", "eventos"]], on="fecha")
    festivo = m["festivo"] != ""
    evento = m["eventos"] != ""
    fin_de_semana = m["dia_semana"].isin(["sáb", "dom"])
    laborable = ~fin_de_semana & ~festivo & ~evento

    def media(mascara: pd.Series) -> float | None:
        return float(m.loc[mascara, "indice"].mean()) if mascara.any() else None

    return {
        "laborables_sin_evento": media(laborable),
        "fines_de_semana": media(fin_de_semana & ~festivo),
        "festivos": media(festivo),
        "dias_con_evento": media(evento),
        "desviacion": float(m["indice"].std(ddof=0)),
        "minimo": float(m["indice"].min()),
        "maximo": float(m["indice"].max()),
    }


def _fmt(valor: float | None) -> str:
    return "n/d" if valor is None else f"{valor:.2f}"


def formatear_resumen(resumen: dict[str, float | None]) -> str:
    return "\n".join(
        [
            "Coherencia del reparto (índice medio; la media del trimestre es 1.00):",
            f"  laborables sin evento  {_fmt(resumen['laborables_sin_evento'])}",
            f"  fines de semana        {_fmt(resumen['fines_de_semana'])}",
            f"  festivos               {_fmt(resumen['festivos'])}",
            f"  días con evento        {_fmt(resumen['dias_con_evento'])}",
            f"  dispersión (σ) {_fmt(resumen['desviacion'])} | mín {_fmt(resumen['minimo'])} | máx {_fmt(resumen['maximo'])}",
        ]
    )
