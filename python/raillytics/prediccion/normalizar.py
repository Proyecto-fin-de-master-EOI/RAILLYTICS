"""Del índice relativo del LLM a viajeros por día, con suma exacta (funciones puras).

Se usa `Fraction` (aritmética racional exacta) para que el reparto sea determinista y la suma
coincida con el total esperado sin errores de coma flotante.
"""
from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from fractions import Fraction

# Límites duros del índice que devuelve el LLM (1.0 = día laborable típico).
INDICE_MIN = 0.2
INDICE_MAX = 3.0


@dataclass(frozen=True)
class IndiceDia:
    fecha: date
    indice: float
    motivo: str


def _pesos(indices: Sequence[float]) -> list[Fraction]:
    if not indices:
        raise ValueError("no hay índices que repartir")
    pesos = [Fraction(i) for i in indices]
    if any(p < 0 for p in pesos):
        raise ValueError("los índices no pueden ser negativos")
    if sum(pesos) <= 0:
        raise ValueError("la suma de los índices debe ser positiva")
    return pesos


def repartir(indices: Sequence[float], total: int) -> list[int]:
    """Reparte `total` en enteros proporcionales a `indices`; la suma es exactamente `total`.

    Método del mayor resto: cada día recibe la parte entera de su cuota y los viajeros que sobran
    van, de uno en uno, a los días con mayor parte decimal (empate: el día más temprano).
    """
    if total < 0:
        raise ValueError("el total a repartir no puede ser negativo")
    pesos = _pesos(indices)
    suma = sum(pesos)
    cuotas = [Fraction(total) * p / suma for p in pesos]
    reparto = [math.floor(c) for c in cuotas]
    sobran = total - sum(reparto)
    orden = sorted(range(len(cuotas)), key=lambda i: (-(cuotas[i] - reparto[i]), i))
    for i in orden[:sobran]:
        reparto[i] += 1
    return reparto


def normalizar_indices(indices: Sequence[float]) -> list[float]:
    """Reescala los índices para que su media sea 1.0."""
    pesos = _pesos(indices)
    suma = sum(pesos)
    return [float(p * len(pesos) / suma) for p in pesos]
