"""Total esperado de demanda del corredor en un trimestre objetivo (función pura).

Sea T el trimestre objetivo y U el último trimestre publicado ESTRICTAMENTE anterior a T:

    total_esperado(T) = total(T-4) × total(U) / total(U-4)

Es el mismo trimestre del año anterior por el crecimiento interanual del último publicado.
Como U nunca mira a T, relanzar un trimestre ya publicado es un backtest sin fuga de datos.

Esa propiedad es la que permite poner un INTERVALO a la previsión: se re-predicen los trimestres ya
publicados, se mide el error que cometió la regla y los cuantiles de esos errores dan la banda. No se
supone ninguna distribución; son los errores observados.
"""
from __future__ import annotations

import math
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


# Un punto del backtest solo vale si sus TRES entradas son de operación normal. Durante la pandemia
# la demanda cayó por razones ajenas al calendario, así que un trimestre cuya base (T-4) o cuya
# referencia de crecimiento (U-4) caiga ahí no mide el error de la regla: mide la pandemia.
ANIOS_ANOMALOS = (2020, 2021)
CONFIANZA_POR_DEFECTO = 0.90
# Con menos puntos los cuantiles del 5 % y el 95 % los decide un solo trimestre.
MINIMO_PUNTOS = 8


@dataclass(frozen=True)
class Intervalo:
    """Banda de confianza del total trimestral, con lo que hace falta para juzgarla."""

    bajo: int
    alto: int
    confianza: float
    puntos: int          # trimestres del backtest que la sostienen
    sesgo: float         # error medio de la regla: positivo = tiende a pasarse
    error_absoluto: float

    def describir(self) -> str:
        return (
            f"IC {self.confianza:.0%}: {miles(self.bajo)} a {miles(self.alto)} viajeros "
            f"(de {self.puntos} trimestres de backtest; la regla se pasa un {self.sesgo:+.1%} de media, "
            f"error absoluto medio {self.error_absoluto:.1%})"
        )


def errores_backtest(trimestrales: Mapping[Trimestre, int],
                     anios_anomalos: tuple[int, ...] = ANIOS_ANOMALOS) -> list[tuple[Trimestre, float]]:
    """Error relativo de la regla en cada trimestre ya publicado que se puede re-predecir.

    Error positivo = la regla previó más viajeros de los que hubo.
    """
    salida = []
    for objetivo in sorted(trimestrales):
        try:
            nivel = calcular_nivel(trimestrales, objetivo)
        except NivelError:
            continue
        entradas = (objetivo, nivel.base, nivel.ultimo, nivel.referencia)
        if any(t.anio in anios_anomalos for t in entradas):
            continue
        real = trimestrales[objetivo]
        if real <= 0:
            continue
        salida.append((objetivo, nivel.total / real - 1))
    return salida


def _cuantil(ordenados: list[float], q: float) -> float:
    """Cuantil por interpolación lineal, sin traerse numpy a un módulo que es función pura."""
    if not ordenados:
        raise NivelError("no hay errores con los que calcular el cuantil")
    pos = q * (len(ordenados) - 1)
    bajo = int(pos)
    alto = min(bajo + 1, len(ordenados) - 1)
    return ordenados[bajo] + (ordenados[alto] - ordenados[bajo]) * (pos - bajo)


def _extremos(ordenados: list[float], confianza: float) -> tuple[float, float]:
    """Los dos cuantiles del error, con la correccion de muestra finita.

    Con los cuantiles empiricos a secas el intervalo sale OPTIMISTA: medido sobre los trimestres del
    backtest, uno que decia 90 % cubria el 79 %. La razon es que con pocas decenas de puntos los
    extremos los decide un punado de valores de la cola.

    La correccion es la de prediccion conforme: se infla el nivel del cuantil por (n+1)/n, que es lo
    que da garantia de cobertura en muestra finita en vez de asintotica. Con n=24 el 95 % pasa a
    pedir casi el maximo observado, y eso es lo honesto: con 24 trimestres no se puede ser mas fino.
    """
    n = len(ordenados)
    margen = (1 - confianza) / 2
    # Estadisticos de orden de la prediccion conforme. Multiplicar el NIVEL del cuantil por (n+1)/n
    # no vale: ensancha por arriba pero ESTRECHA por abajo, porque sube los dos niveles a la vez.
    # Con el suelo y el techo sobre el rango se ensancha por los dos lados, que es lo que hace falta.
    nivel_bajo = math.floor((n + 1) * margen) / n
    nivel_alto = math.ceil((n + 1) * (1 - margen)) / n
    return (_cuantil(ordenados, max(0.0, min(1.0, nivel_bajo))),
            _cuantil(ordenados, max(0.0, min(1.0, nivel_alto))))


def intervalo(nivel: NivelEsperado, trimestrales: Mapping[Trimestre, int],
              confianza: float = CONFIANZA_POR_DEFECTO) -> Intervalo:
    """Banda del total esperado, a partir de los errores que la regla cometió en el pasado.

    La previsión se DIVIDE por (1 + error) y no se multiplica: el error se definió como
    `previsto/real - 1`, así que deshacerlo es dividir. Multiplicar daría una banda al revés y
    desplazada, que es el fallo clásico aquí.

    El cuantil alto del error da el extremo BAJO de la banda, porque un error de +14 % significa que
    la regla se pasó: el real estaba por debajo.
    """
    errores = [e for _, e in errores_backtest(trimestrales)]
    if len(errores) < MINIMO_PUNTOS:
        raise NivelError(
            f"solo hay {len(errores)} trimestres de backtest utilizables y hacen falta {MINIMO_PUNTOS} "
            "para estimar los cuantiles del error: no se puede dar un intervalo honesto"
        )
    errores.sort()
    e_bajo, e_alto = _extremos(errores, confianza)
    medio = sum(errores) / len(errores)
    return Intervalo(
        bajo=int(round(nivel.total / (1 + e_alto))),
        alto=int(round(nivel.total / (1 + e_bajo))),
        confianza=confianza,
        puntos=len(errores),
        sesgo=medio,
        error_absoluto=sum(abs(e) for e in errores) / len(errores),
    )


def cobertura(trimestrales: Mapping[Trimestre, int],
              confianza: float = CONFIANZA_POR_DEFECTO) -> float:
    """Qué parte de los trimestres del backtest habría caído dentro de su propia banda.

    Es la comprobación que convierte el intervalo en algo defendible: si dice 90 % y cubre el 65 %,
    está mintiendo. Se calcula dejando fuera cada trimestre al estimar los cuantiles (uno fuera),
    porque incluirlo haría que el intervalo se evaluara contra un error que ayudó a definir.
    """
    puntos = errores_backtest(trimestrales)
    if len(puntos) <= MINIMO_PUNTOS:
        raise NivelError("no hay bastantes trimestres para medir la cobertura dejando uno fuera")
    dentro = 0
    for i, (_, error) in enumerate(puntos):
        resto = sorted(e for j, (_, e) in enumerate(puntos) if j != i)
        bajo, alto = _extremos(resto, confianza)
        if bajo <= error <= alto:
            dentro += 1
    return dentro / len(puntos)
