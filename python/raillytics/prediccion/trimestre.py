"""Trimestres naturales (`AAAA-Tn`) y su aritmética."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta

_PATRON = re.compile(r"^(\d{4})-T([1-4])$")


@dataclass(frozen=True, order=True)
class Trimestre:
    anio: int
    numero: int

    def __post_init__(self) -> None:
        if not 1 <= self.numero <= 4:
            raise ValueError(f"número de trimestre fuera de rango: {self.numero}")

    @classmethod
    def parse(cls, texto: str) -> Trimestre:
        coincidencia = _PATRON.match(texto.strip())
        if not coincidencia:
            raise ValueError(f"trimestre inválido {texto!r}: se espera el formato AAAA-Tn (p. ej. 2026-T4)")
        return cls(int(coincidencia.group(1)), int(coincidencia.group(2)))

    @classmethod
    def de_fecha(cls, dia: date) -> Trimestre:
        """El trimestre natural que contiene `dia`."""
        return cls(dia.year, (dia.month - 1) // 3 + 1)

    def __str__(self) -> str:
        return f"{self.anio}-T{self.numero}"

    def _indice(self) -> int:
        return self.anio * 4 + (self.numero - 1)

    def mas(self, n: int) -> Trimestre:
        indice = self._indice() + n
        return Trimestre(indice // 4, indice % 4 + 1)

    def menos(self, n: int) -> Trimestre:
        return self.mas(-n)

    def distancia(self, otro: Trimestre) -> int:
        """Trimestres que separan `self` de `otro` (positivo si `self` es posterior)."""
        return self._indice() - otro._indice()

    @property
    def inicio(self) -> date:
        return date(self.anio, 3 * (self.numero - 1) + 1, 1)

    @property
    def fin(self) -> date:
        return self.mas(1).inicio - timedelta(days=1)

    def dias(self) -> list[date]:
        total = (self.fin - self.inicio).days + 1
        return [self.inicio + timedelta(days=i) for i in range(total)]
