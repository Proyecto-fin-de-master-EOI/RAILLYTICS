"""Modo eventos de la predicción (PRED_PROMPT=eventos_vN): el código reparte el trimestre con reglas fijas y el LLM solo
valora los eventos.

Con demanda_v3/v4 el LLM debía aplicar reglas numéricas día a día y, aunque viera el contexto, no siempre hacía la suma
(README, «El prompt»). Aquí el valor de cada día sale de config/reglas_demanda.yml: valor base por día de la semana,
festivo entre semana y puente, más los ajustes de víspera, regreso y día junto a un evento (`tipos_de_dia`). El LLM solo
recibe los días con evento y devuelve un factor por día (1.00 = el evento no cambia la demanda) que multiplica ese valor.
"""
from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

import yaml

from raillytics.prediccion.calendario import SEMANA, Calendario, tipos_de_dia
from raillytics.prediccion.gates import DIAS_SEMANA
from raillytics.prediccion.normalizar import IndiceDia

logger = logging.getLogger(__name__)

PREFIJO_MODO_EVENTOS = "eventos_"
RUTA_POR_DEFECTO = Path("config/reglas_demanda.yml")
# El ajuste de «junto a un evento» solo se suma si algún evento vecino mueve viajeros según el LLM: con un evento que
# valora en 1.00, subir el día de antes y el de después contradiría su propia valoración.
FACTOR_MIN_JUNTO = 1.05
MESES = ("ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic")
# `base_mes` es opcional: sin él el reparto trata todos los meses de un trimestre al mismo nivel, que es como funcionó
# hasta que hubo un dato mensual con el que medirlo (raillytics.prediccion.perfil_mensual).
MES_NEUTRO = 1.0


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
    base_mes: Mapping[str, float]

    def como_dict(self) -> dict:
        """Para la trazabilidad: las reglas con las que se calculó cada ejecución."""
        return {**asdict(self), "base_dia_semana": dict(self.base_dia_semana), "base_mes": dict(self.base_mes)}


def es_modo_eventos(version_prompt: str) -> bool:
    return version_prompt.startswith(PREFIJO_MODO_EVENTOS)


def cargar_reglas(ruta: Path) -> Reglas:
    if not ruta.is_file():
        raise ReglasError(f"no existe el fichero de reglas {ruta} (REGLAS_DEMANDA)")
    datos = yaml.safe_load(ruta.read_text(encoding="utf-8")) or {}
    # Fuera del try de abajo a propósito: ese convierte cualquier fallo en el mensaje de las claves
    # obligatorias, y un `base_mes` mal escrito necesita que se diga que es `base_mes`.
    base_mes = _base_mes(datos, ruta)
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
            base_mes=base_mes,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ReglasError(f"{ruta}: falta o no es un número {exc}. Claves: base_dia_semana (lun..dom), "
                          "festivo_entre_semana, puente y ajustes (vispera, regreso, junto_a_evento)") from exc


def _base_mes(datos: Mapping[str, object], ruta: Path) -> Mapping[str, float]:
    """El perfil mensual del fichero, o todos los meses a 1.00 si no lo declara.

    Es opcional porque llegó después: hasta que hubo un dato mensual con el que medirlo, el reparto
    trataba igual a todos los meses de un trimestre. Pero si está, tiene que estar completo: con
    cuatro meses escritos y ocho olvidados el reparto saldría sesgado hacia los que faltan y nada lo
    diría. O los doce o ninguno.
    """
    declarado = datos.get("base_mes")
    if declarado is None:
        logger.info("%s no declara base_mes: el reparto trata igual a todos los meses del trimestre", ruta)
        return {mes: MES_NEUTRO for mes in MESES}
    if not isinstance(declarado, Mapping):
        raise ReglasError(f"{ruta}: base_mes tiene que ser un mapa de mes a número, no {type(declarado).__name__}")
    faltan = [mes for mes in MESES if mes not in declarado]
    if faltan:
        raise ReglasError(f"{ruta}: base_mes tiene que declarar los doce meses ({', '.join(MESES)}); "
                          f"faltan {', '.join(faltan)}")
    sobran = sorted(set(declarado) - set(MESES))
    if sobran:
        raise ReglasError(f"{ruta}: base_mes no reconoce {', '.join(map(str, sobran))}; los meses son "
                          f"{', '.join(MESES)}")
    try:
        return {mes: float(declarado[mes]) for mes in MESES}
    except (TypeError, ValueError) as exc:
        raise ReglasError(f"{ruta}: base_mes tiene un valor que no es un número ({exc})") from exc


def perfil_mensual(indices: Sequence[IndiceDia], reglas: Reglas) -> list[IndiceDia]:
    """Escala el valor de cada día por el coeficiente de su mes.

    Va en un paso aparte, después de `indices_base` y de `combinar`, y no dentro de ellos, porque es
    una cosa distinta: `indices_base` responde «qué clase de día es este» y el perfil responde «a qué
    nivel está este mes». Separarlo deja las reglas de clase de día verificables por sí solas y hace
    que el factor del mes escale también el ajuste de los días junto a un evento y el factor del LLM,
    que es lo que corresponde a un nivel.

    El reparto se normaliza después contra el total trimestral de CNMC, así que esto solo cambia el
    PESO RELATIVO de los meses dentro del trimestre, nunca el total, que es dato real.
    """
    resultado = []
    for dia in indices:
        factor = reglas.base_mes[MESES[dia.fecha.month - 1]]
        motivo = dia.motivo if factor == MES_NEUTRO else f"{dia.motivo}, perfil de {MESES[dia.fecha.month - 1]} ({factor:g})"
        resultado.append(IndiceDia(dia.fecha, round(dia.indice * factor, 6), motivo))
    return resultado


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
