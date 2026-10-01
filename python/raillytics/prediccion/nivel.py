"""Total esperado de demanda del corredor en un trimestre objetivo (función pura).

Sea T el trimestre objetivo y U el último trimestre publicado ESTRICTAMENTE anterior a T:

    total_esperado(T) = total(T-4) × total(U) / total(U-4)

Es el mismo trimestre del año anterior por el crecimiento interanual del último publicado.
Como U nunca mira a T, relanzar un trimestre ya publicado es un backtest sin fuga de datos.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from fractions import Fraction

from raillytics.prediccion.trimestre import Trimestre


class NivelError(ValueError):
    """No se puede calcular el nivel con los trimestres publicados."""


def miles(n: int) -> str:
    """1320000 -> '1.320.000' (separador de miles español)."""
    return f"{n:,}".replace(",", ".")


@dataclass(frozen=True)
class NivelEsperado:
    objetivo: Trimestre
    total: int
    base: Trimestre        # T-4: mismo trimestre del año anterior
    ultimo: Trimestre      # U: último trimestre publicado anterior a T
    referencia: Trimestre  # U-4
    total_base: int
    total_ultimo: int
    total_referencia: int

    @property
    def crecimiento(self) -> float:
        return self.total_ultimo / self.total_referencia - 1

    def describir(self) -> str:
        return (
            f"{self.objetivo}: {miles(self.total)} viajeros esperados = {self.base} ({miles(self.total_base)}) "
            f"× {self.ultimo}/{self.referencia} ({self.crecimiento:+.1%})"
        )


def calcular_nivel(trimestrales: Mapping[Trimestre, int], objetivo: Trimestre) -> NivelEsperado:
    anteriores = [t for t in trimestrales if t < objetivo]
    if not anteriores:
        raise NivelError(f"no hay ningún trimestre publicado anterior a {objetivo}")
    ultimo = max(anteriores)
    if objetivo.distancia(ultimo) > 4:
        raise NivelError(
            f"{objetivo} está a más de 4 trimestres del último publicado ({ultimo}): no hay un mismo "
            "trimestre del año anterior publicado con el que calcular el nivel. "
            "Pasa --total-esperado N para fijarlo a mano"
        )
    base, referencia = objetivo.menos(4), ultimo.menos(4)
    faltan = [t for t in (base, referencia) if t not in trimestrales]
    if faltan:
        raise NivelError(
            f"faltan trimestres publicados para calcular el nivel de {objetivo}: "
            + ", ".join(str(t) for t in faltan)
            + ". Pasa --total-esperado N para fijarlo a mano"
        )
    total_base, total_ultimo, total_referencia = trimestrales[base], trimestrales[ultimo], trimestrales[referencia]
    if min(total_base, total_ultimo, total_referencia) <= 0:
        raise NivelError(
            f"los totales de {base}, {ultimo} y {referencia} deben ser positivos "
            f"(son {total_base}, {total_ultimo} y {total_referencia})"
        )
    total = round(Fraction(total_base * total_ultimo, total_referencia))
    return NivelEsperado(objetivo, total, base, ultimo, referencia, total_base, total_ultimo, total_referencia)


def desviacion_relativa(esperado: int, real: int) -> float:
    """(esperado - real) / real: positivo si el nivel esperado se pasó."""
    if real <= 0:
        raise NivelError(f"el total real debe ser positivo (es {real})")
    return (esperado - real) / real
