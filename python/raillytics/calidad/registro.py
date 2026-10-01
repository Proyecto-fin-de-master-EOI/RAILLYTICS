"""Registro de resultados de Quality Gates (lado Python).

Cada evaluación de gates deja una fila por gate en una tabla Parquet del lake,
hermana de la trazabilidad de cargas y con el mismo run_id, para poder cruzarlas:

    <gold_root>/_trazabilidad/calidad/<run_id>.parquet   (s3://raillytics-gold/... por defecto)

Columnas (las mismas que escribe raillytics.common.calidad.QualityGates en Scala):
run_id, proceso, capa, tabla, gate, tipo, severidad ('bloqueante' | 'aviso'),
resultado ('ok' | 'fallo' | 'error'), valor (métrica observada), umbral, detalle,
inicio y fin (UTC), duracion_s, lanzado_por, ejecutor y usuario.

'error' es un gate que no se pudo evaluar: cuenta como fallo (fail closed).

CLI:  python -m raillytics.calidad [--limit N]    -> últimos resultados registrados
"""
from __future__ import annotations

import argparse
import getpass
import logging
import socket
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone

import duckdb

from raillytics.utils.cargas import lanzado_por_defecto
from raillytics.utils.lake import LakeLayout, S3Settings, connect, ensure_parent_dir

logger = logging.getLogger(__name__)

SEVERIDAD_BLOQUEANTE = "bloqueante"
SEVERIDAD_AVISO = "aviso"
RESULTADO_OK = "ok"
RESULTADO_FALLO = "fallo"
RESULTADO_ERROR = "error"

# Columnas y tipos DuckDB de la tabla de calidad (mismo esquema que QualityGates.Schema en Scala).
CALIDAD_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "VARCHAR"),
    ("proceso", "VARCHAR"),
    ("capa", "VARCHAR"),
    ("tabla", "VARCHAR"),
    ("gate", "VARCHAR"),
    ("tipo", "VARCHAR"),
    ("severidad", "VARCHAR"),
    ("resultado", "VARCHAR"),
    ("valor", "DOUBLE"),
    ("umbral", "VARCHAR"),
    ("detalle", "VARCHAR"),
    ("inicio", "TIMESTAMP"),
    ("fin", "TIMESTAMP"),
    ("duracion_s", "DOUBLE"),
    ("lanzado_por", "VARCHAR"),
    ("ejecutor", "VARCHAR"),
    ("usuario", "VARCHAR"),
)


def _ahora() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass
class ResultadoGate:
    """Resultado de un gate sobre una tabla o fichero."""

    tabla: str
    gate: str
    tipo: str
    severidad: str
    resultado: str
    valor: float | None = None
    umbral: str | None = None
    detalle: str | None = None
    inicio: datetime = field(default_factory=_ahora)
    fin: datetime | None = None

    @property
    def pasa(self) -> bool:
        return self.resultado == RESULTADO_OK

    @property
    def bloquea(self) -> bool:
        """Un fallo (o error de evaluación) bloqueante impide promocionar la carga."""
        return not self.pasa and self.severidad == SEVERIDAD_BLOQUEANTE


class QualityGateError(Exception):
    """Al menos un gate bloqueante ha fallado; la carga no debe promocionarse."""

    def __init__(self, resultados: Sequence[ResultadoGate]):
        self.resultados = list(resultados)
        self.fallidos = [r for r in resultados if r.bloquea]
        super().__init__(describir_fallos(self.fallidos))


def describir_fallos(fallidos: Sequence[ResultadoGate]) -> str:
    partes = [
        f"{r.tabla}.{r.gate}" + (f" (valor={r.valor:g}, umbral='{r.umbral}')" if r.valor is not None else "")
        + (f": {r.detalle}" if r.detalle else "")
        for r in fallidos
    ]
    return f"{len(fallidos)} quality gate(s) bloqueante(s) fallido(s): " + "; ".join(partes)


def exigir(resultados: Sequence[ResultadoGate]) -> None:
    """Lanza QualityGateError si algún gate bloqueante no ha pasado."""
    if any(r.bloquea for r in resultados):
        raise QualityGateError(resultados)


def filas_registro(
    resultados: Sequence[ResultadoGate], run_id: str, proceso: str, capa: str, lanzado_por: str | None = None
) -> list[tuple]:
    """Filas en el orden de CALIDAD_COLUMNS."""
    lanzado_por = lanzado_por or lanzado_por_defecto()
    ejecutor = socket.gethostname()
    try:
        usuario = getpass.getuser()
    except Exception:  # sin entrada en /etc/passwd (contenedores)
        usuario = None
    filas = []
    for r in resultados:
        fin = r.fin or _ahora()
        filas.append(
            (
                run_id, proceso, capa, r.tabla, r.gate, r.tipo, r.severidad, r.resultado, r.valor, r.umbral,
                r.detalle[:2000] if r.detalle else None, r.inicio, fin, (fin - r.inicio).total_seconds(),
                lanzado_por, ejecutor, usuario,
            )
        )
    return filas


def escribir_registro(con: duckdb.DuckDBPyConnection, destino: str, filas: list[tuple]) -> None:
    ensure_parent_dir(destino)
    columnas = ", ".join(f"{nombre} {tipo}" for nombre, tipo in CALIDAD_COLUMNS)
    marcadores = ", ".join("?" for _ in CALIDAD_COLUMNS)
    con.execute(f"CREATE OR REPLACE TEMP TABLE registro_calidad ({columnas})")
    con.executemany(f"INSERT INTO registro_calidad VALUES ({marcadores})", filas)
    con.execute(f"COPY registro_calidad TO '{destino}' (FORMAT PARQUET)")
    con.execute("DROP TABLE registro_calidad")


def registrar_calidad(
    resultados: Sequence[ResultadoGate],
    proceso: str,
    capa: str,
    run_id: str,
    layout: LakeLayout | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
    lanzado_por: str | None = None,
) -> None:
    """Escribe los resultados en el lake. Best effort: si falla, queda en el log y no rompe la carga.

    run_id es el de la carga a la que pertenecen (raillytics.utils.cargas.Ejecucion.run_id),
    así el dashboard puede cruzar cada carga con sus gates.
    """
    if not resultados:
        return
    try:
        layout = layout or LakeLayout.from_env()
        con = con or connect(S3Settings.from_env() if layout.uses_s3 else None)
        destino = layout.calidad_file(run_id)
        escribir_registro(con, destino, filas_registro(resultados, run_id, proceso, capa, lanzado_por))
        logger.info("%d resultado(s) de quality gates registrados en %s", len(resultados), destino)
    except Exception:
        logger.exception("no se pudieron registrar los resultados de quality gates de %s", run_id)


def leer_calidad(con: duckdb.DuckDBPyConnection, layout: LakeLayout, limit: int = 30) -> list[tuple]:
    """Últimos resultados registrados (más recientes primero)."""
    return con.execute(
        "SELECT inicio, proceso, capa, tabla, gate, severidad, resultado, valor, umbral, detalle, run_id "
        f"FROM read_parquet('{layout.calidad_glob()}', union_by_name = true) "
        "ORDER BY inicio DESC, tabla, gate LIMIT ?",
        [limit],
    ).fetchall()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m raillytics.calidad",
        description="Muestra los últimos resultados de quality gates registrados en el lake",
    )
    parser.add_argument("--limit", type=int, default=30, help="número de filas (por defecto 30)")
    args = parser.parse_args(argv)

    layout = LakeLayout.from_env()
    con = connect(S3Settings.from_env() if layout.uses_s3 else None)
    try:
        filas = leer_calidad(con, layout, args.limit)
    except duckdb.IOException:
        print(f"Sin resultados de quality gates en {layout.calidad_glob()}")
        return 0
    print(f"{'inicio (UTC)':<21}{'proceso':<24}{'capa':<8}{'tabla':<32}{'gate':<28}{'sev.':<12}{'result.':<8}{'valor':>10}  umbral")
    for inicio, proceso, capa, tabla, gate, severidad, resultado, valor, umbral, detalle, run_id in filas:
        valor_txt = f"{valor:g}" if valor is not None else "-"
        print(
            f"{inicio:%Y-%m-%d %H:%M:%S}  {proceso:<24}{capa:<8}{tabla:<32}{gate:<28}{severidad:<12}{resultado:<8}"
            f"{valor_txt:>10}  {umbral or '-'}" + (f"\n{'':>21}{detalle}" if detalle and resultado != RESULTADO_OK else "")
        )
    return 0
