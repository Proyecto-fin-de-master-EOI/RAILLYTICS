"""Modo eventos de la predicción (PRED_PROMPT=eventos_vN): el código reparte el trimestre con reglas fijas y el LLM solo
valora los eventos.

Con demanda_v3/v4 el LLM debía aplicar reglas numéricas día a día y, aunque viera el contexto, no siempre hacía la suma
(README, «El prompt»). Aquí el valor de cada día sale de config/reglas_demanda.yml: valor base por día de la semana,
festivo entre semana y puente, más los ajustes de víspera, regreso y día junto a un evento (`tipos_de_dia`). El LLM solo
recibe los días con evento y devuelve un factor por día (1.00 = el evento no cambia la demanda) que multiplica ese valor.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

import yaml

from raillytics.prediccion.calendario import SEMANA, Calendario, tipos_de_dia
from raillytics.prediccion.gates import DIAS_SEMANA
from raillytics.prediccion.normalizar import IndiceDia

PREFIJO_MODO_EVENTOS = "eventos_"
RUTA_POR_DEFECTO = Path("config/reglas_demanda.yml")
# El ajuste de «junto a un evento» solo se suma si algún evento vecino mueve viajeros según el LLM: con un evento que
# valora en 1.00, subir el día de antes y el de después contradiría su propia valoración.
FACTOR_MIN_JUNTO = 1.05


class ReglasError(ValueError):
    """El fichero de reglas no existe o no tiene la forma esperada."""


@dataclass(frozen=True)
class Reglas:
    base_dia_semana: Mapping[str, float]
    festivo_entre_semana: float
    puente: float
    vispera: float
    regreso: float
    junto_a_evento: float

    def como_dict(self) -> dict:
        """Para la trazabilidad: las reglas con las que se calculó cada ejecución."""
        return {**asdict(self), "base_dia_semana": dict(self.base_dia_semana)}


def es_modo_eventos(version_prompt: str) -> bool:
    return version_prompt.startswith(PREFIJO_MODO_EVENTOS)


def cargar_reglas(ruta: Path) -> Reglas:
    if not ruta.is_file():
        raise ReglasError(f"no existe el fichero de reglas {ruta} (REGLAS_DEMANDA)")
    datos = yaml.safe_load(ruta.read_text(encoding="utf-8")) or {}
    try:
        base = {dia: float(datos["base_dia_semana"][dia]) for dia in SEMANA}
        ajustes = datos["ajustes"]
        return Reglas(
            base_dia_semana=base,
            festivo_entre_semana=float(datos["festivo_entre_semana"]),
            puente=float(datos["puente"]),
            vispera=float(ajustes["vispera"]),
            regreso=float(ajustes["regreso"]),
            junto_a_evento=float(ajustes["junto_a_evento"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ReglasError(f"{ruta}: falta o no es un número {exc}. Claves: base_dia_semana (lun..dom), "
                          "festivo_entre_semana, puente y ajustes (vispera, regreso, junto_a_evento)") from exc


def indices_base(calendario: Calendario, reglas: Reglas) -> list[IndiceDia]:
    """El valor de cada día por las reglas, sin el ajuste de «junto a un evento» (depende de la valoración del LLM)."""
    resultado = []
    for fila in tipos_de_dia(calendario).itertuples(index=False):
        entre_semana = fila.dia_semana not in ("sáb", "dom")
        partes = []
        if fila.festivo:
            partes.append(f"festivo ({fila.festivo})")
        if fila.festivo and entre_semana:
            valor = reglas.festivo_entre_semana
        elif fila.puente:
            valor = reglas.puente
            partes.append("puente")
        else:
            valor = reglas.base_dia_semana[fila.dia_semana]
        if fila.vispera:
            valor += reglas.vispera
            partes.append("víspera de tramo festivo")
        if fila.regreso:
            valor += reglas.regreso
            partes.append("regreso de tramo festivo")
        nombre = DIAS_SEMANA[fila.fecha.weekday()]
        motivo = f"{nombre} {', '.join(partes)}" if partes else f"{nombre} {'laborable' if entre_semana else 'de fin de semana'}"
        resultado.append(IndiceDia(fila.fecha, round(valor, 6), motivo))
    return resultado


def combinar(
    base: Sequence[IndiceDia], factores: Sequence[IndiceDia], calendario: Calendario, reglas: Reglas
) -> list[IndiceDia]:
    """Aplica el factor del LLM a cada día con evento y el ajuste fijo a los días junto a un evento que mueve viajeros.

    En los días con evento el motivo empieza por el del LLM: así el gate del día de la semana revisa lo que escribió él.
    """
    por_fecha = {f.fecha: f for f in factores}
    tipos = tipos_de_dia(calendario)
    fechas = list(tipos["fecha"])
    faltan = sorted(set(tipos.loc[tipos["eventos"] != "", "fecha"]) - set(por_fecha))
    if faltan:
        raise ValueError(f"faltan los factores del LLM de los días con evento: {', '.join(d.isoformat() for d in faltan)}")
    resultado = []
    for i, (dia, junto) in enumerate(zip(base, tipos["junto_a_evento"])):
        valor, motivo = dia.indice, dia.motivo
        if junto:
            vecinos = [por_fecha[fechas[j]].indice for j in (i - 1, i + 1) if 0 <= j < len(fechas) and fechas[j] in por_fecha]
            if vecinos and max(vecinos) >= FACTOR_MIN_JUNTO:
                valor += reglas.junto_a_evento
                motivo = f"{motivo}, junto a un evento"
        if dia.fecha in por_fecha:
            factor = por_fecha[dia.fecha]
            valor *= factor.indice
            motivo = f"{factor.motivo} · {motivo}"
        resultado.append(IndiceDia(dia.fecha, round(valor, 6), motivo))
    return resultado
