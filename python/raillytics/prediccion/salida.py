"""Salida de la predicción: DataFrame final, CSV atómico y resumen de coherencia del reparto."""
from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from raillytics.prediccion.calendario import Calendario, tipos_de_dia
from raillytics.prediccion.normalizar import IndiceDia
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.fs import atomic_write_bytes

CORREDOR = "AVE-MAD-BCN"
# Dónde se escribe el CSV si no hay PREDICCIONES_ROOT (relativa a donde se lanza; `make` lo hace desde la raíz del repo).
# Es un directorio del repo que está en git, a propósito fuera de data/ (que se ignora): los resultados se versionan.
RAIZ_POR_DEFECTO = Path("resultados/predicciones")
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
    try:
        destino.parent.mkdir(parents=True, exist_ok=True)
    except PermissionError as exc:
        raise PermissionError(
            f"no se puede crear {destino.parent}: {exc.strerror}. Si corre dentro de Airflow, fija AIRFLOW_UID=$(id -u) "
            "en el .env y recrea el stack (make down && make up) para que escriba con tu usuario"
        ) from exc
    contenido = df.to_csv(index=False, float_format="%.6f", lineterminator="\n").encode("utf-8")
    return atomic_write_bytes(destino, contenido)


def contexto_por_dia(df: pd.DataFrame, calendario: Calendario) -> pd.DataFrame:
    """Por cada día de `df` (fecha ISO e índice): su calendario, las marcas de `tipos_de_dia` y su `exceso`.

    El exceso es el índice menos el medio de los días corrientes (sin festivo, evento ni ninguna marca) con el mismo
    día de la semana. Mide si el LLM aplica las reglas que dependen de los días de alrededor: la media cruda no vale,
    porque casi todas las vísperas son viernes y casi todos los regresos domingos, así que saldría alta aunque el LLM
    las ignorara. NaN en los festivos entre semana (tienen su propio valor base) y si no hay día corriente con el que
    comparar. Lo usan el resumen de cada ejecución y la tabla Gold, para que consola y dashboard den lo mismo.
    """
    cal = tipos_de_dia(calendario)
    cal = cal.assign(fecha=cal["fecha"].map(lambda d: d.isoformat()))
    columnas = ["fecha", "dia_semana", "festivo", "eventos", "puente", "vispera", "regreso", "junto_a_evento"]
    m = df[["fecha", "indice"]].merge(cal[columnas], on="fecha")
    festivo = m["festivo"] != ""
    corriente = ~festivo & (m["eventos"] == "") & ~(m["vispera"] | m["puente"] | m["regreso"] | m["junto_a_evento"])
    exceso = m["indice"] - m["dia_semana"].map(m.loc[corriente].groupby("dia_semana")["indice"].mean())
    festivo_entre_semana = festivo & ~m["dia_semana"].isin(["sáb", "dom"])
    return m.assign(exceso=exceso.mask(festivo_entre_semana))


def resumen_coherencia(df: pd.DataFrame, calendario: Calendario) -> dict[str, float | None]:
    """Índice medio por tipo de día. No hay verdad externa: sirve para comparar versiones del prompt.

    Para vísperas, puentes, regresos y días junto a un evento da su exceso medio sobre un día corriente del mismo día
    de la semana (ver `contexto_por_dia`): 0.00 = la regla no se aplica.
    """
    m = contexto_por_dia(df, calendario)
    festivo = m["festivo"] != ""
    evento = m["eventos"] != ""
    fin_de_semana = m["dia_semana"].isin(["sáb", "dom"])
    laborable = ~fin_de_semana & ~festivo & ~evento

    def media(mascara: pd.Series) -> float | None:
        return float(m.loc[mascara, "indice"].mean()) if mascara.any() else None

    def exceso_medio(mascara: pd.Series) -> float | None:
        valores = m.loc[mascara, "exceso"].dropna()
        return float(valores.mean()) if len(valores) else None

    return {
        "laborables_sin_evento": media(laborable),
        "fines_de_semana": media(fin_de_semana & ~festivo),
        "festivos": media(festivo),
        "dias_con_evento": media(evento),
        "exceso_visperas": exceso_medio(m["vispera"]),
        "exceso_puentes": exceso_medio(m["puente"]),
        "exceso_regresos": exceso_medio(m["regreso"]),
        "exceso_junto_a_evento": exceso_medio(m["junto_a_evento"]),
        "desviacion": float(m["indice"].std(ddof=0)),
        "minimo": float(m["indice"].min()),
        "maximo": float(m["indice"].max()),
    }


def _fmt(valor: float | None, signo: bool = False) -> str:
    return "n/d" if valor is None else f"{valor:+.2f}" if signo else f"{valor:.2f}"


def formatear_resumen(resumen: dict[str, float | None]) -> str:
    return "\n".join(
        [
            "Coherencia del reparto (índice medio; la media del trimestre es 1.00):",
            f"  laborables sin evento  {_fmt(resumen['laborables_sin_evento'])}",
            f"  fines de semana        {_fmt(resumen['fines_de_semana'])}",
            f"  festivos               {_fmt(resumen['festivos'])}",
            f"  días con evento        {_fmt(resumen['dias_con_evento'])}",
            "  exceso sobre un día corriente del mismo día de la semana (+0.00 = la regla no se aplica):",
            f"  vísperas               {_fmt(resumen['exceso_visperas'], signo=True)}",
            f"  puentes                {_fmt(resumen['exceso_puentes'], signo=True)}",
            f"  regresos               {_fmt(resumen['exceso_regresos'], signo=True)}",
            f"  junto a un evento      {_fmt(resumen['exceso_junto_a_evento'], signo=True)}",
            f"  desviación típica {_fmt(resumen['desviacion'])} | mín {_fmt(resumen['minimo'])} | máx {_fmt(resumen['maximo'])}",
        ]
    )
