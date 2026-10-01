"""Trazabilidad de cargas del lake.

Cada proceso de carga (descarga Bronze, Silver, Gold...) deja constancia de
qué ejecutó, cuándo, con qué parámetros y qué produjo, en una tabla Parquet
del lake que DuckDB y Superset consultan igual que a las tablas Gold:

    <gold_root>/_trazabilidad/cargas/<run_id>.parquet   (s3://raillytics-gold/... por defecto)

Una fila por (ejecución, tabla): run_id, proceso, capa, tabla, origen, destino,
filas, bytes, inicio y fin (UTC), duracion_s, estado ('ok' | 'error'), error,
parametros (JSON), lanzado_por ('make' | 'airflow:<dag>' | 'cli'), ejecutor
(host) y usuario. Cada ejecución escribe su propio fichero: en S3 no hay
append, y así dos ejecuciones concurrentes nunca se pisan.

Uso:

    with registrar_carga("gold_build", "gold", layout, con, parametros={...}) as ejecucion:
        with ejecucion.tabla("dim_fecha", origen=..., destino=...) as carga:
            ...                    # la carga propiamente dicha
            carga.filas = 365

El registro se escribe al salir del bloque exterior, también cuando hay un
error (la excepción se propaga). Es "best effort": si el registro no se puede
escribir queda constancia en el log, pero la carga no falla por eso.

Las apps Scala (L1/L2) pueden sumarse escribiendo Parquet con estas mismas
columnas en el mismo prefijo (Spark: .write.mode("append")).

CLI:  python -m raillytics.utils.cargas [--limit N]    -> últimas cargas registradas
"""
from __future__ import annotations

import argparse
import getpass
import json
import logging
import os
import socket
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

import duckdb

from raillytics.utils.lake import LakeLayout, S3Settings, connect, ensure_parent_dir

logger = logging.getLogger(__name__)

# Columnas y tipos DuckDB de la tabla de cargas. Con tipos explícitos todos
# los ficheros comparten esquema aunque una columna venga vacía.
CARGAS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "VARCHAR"),
    ("proceso", "VARCHAR"),
    ("capa", "VARCHAR"),
    ("tabla", "VARCHAR"),
    ("origen", "VARCHAR"),
    ("destino", "VARCHAR"),
    ("filas", "BIGINT"),
    ("bytes", "BIGINT"),
    ("inicio", "TIMESTAMP"),
    ("fin", "TIMESTAMP"),
    ("duracion_s", "DOUBLE"),
    ("estado", "VARCHAR"),
    ("error", "VARCHAR"),
    ("parametros", "VARCHAR"),
    ("lanzado_por", "VARCHAR"),
    ("ejecutor", "VARCHAR"),
    ("usuario", "VARCHAR"),
)

ESTADO_OK = "ok"
ESTADO_ERROR = "error"


def _ahora() -> datetime:
    # UTC sin tzinfo: DuckDB lo guarda como TIMESTAMP; el dashboard lo pasa a hora de Madrid.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _describir(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:2000]


def lanzado_por_defecto(env: Mapping[str, str] = os.environ) -> str:
    if dag_id := env.get("AIRFLOW_CTX_DAG_ID"):
        return f"airflow:{dag_id}"
    if "MAKELEVEL" in env:  # GNU make lo exporta a sus subprocesos
        return "make"
    return "cli"


@dataclass
class CargaTabla:
    """Carga de una tabla dentro de una ejecución; se rellena dentro del bloque with."""

    tabla: str | None
    origen: str | None = None
    destino: str | None = None
    filas: int | None = None
    bytes: int | None = None
    inicio: datetime = field(default_factory=_ahora)
    fin: datetime | None = None
    estado: str = ESTADO_OK
    error: str | None = None


@dataclass
class Ejecucion:
    """Una ejecución de un proceso de carga y las tablas que ha cargado."""

    proceso: str
    capa: str
    parametros: Mapping[str, object] | None = None
    lanzado_por: str = field(default_factory=lanzado_por_defecto)
    inicio: datetime = field(default_factory=_ahora)
    run_id: str = ""
    tablas: list[CargaTabla] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.run_id:
            self.run_id = f"{self.inicio:%Y%m%dT%H%M%S}-{self.proceso}-{uuid.uuid4().hex[:6]}"

    @contextmanager
    def tabla(self, nombre: str, origen: str | None = None, destino: str | None = None) -> Iterator[CargaTabla]:
        carga = CargaTabla(nombre, origen, destino)
        self.tablas.append(carga)
        try:
            yield carga
        except BaseException as exc:
            carga.estado, carga.error = ESTADO_ERROR, _describir(exc)
            raise
        finally:
            carga.fin = _ahora()

    def marcar_error(self, exc: BaseException) -> None:
        """Error fuera de cualquier tabla (p. ej. antes de la primera): fila de ejecución sin tabla."""
        if not any(t.estado == ESTADO_ERROR for t in self.tablas):
            self.tablas.append(
                CargaTabla(None, inicio=self.inicio, fin=_ahora(), estado=ESTADO_ERROR, error=_describir(exc))
            )

    def filas_registro(self) -> list[tuple]:
        """Filas en el orden de CARGAS_COLUMNS; una ejecución sin tablas deja una fila sin tabla."""
        fin = _ahora()
        tablas = self.tablas or [CargaTabla(None, inicio=self.inicio, fin=fin)]
        parametros = (
            json.dumps(self.parametros, ensure_ascii=False, default=str) if self.parametros is not None else None
        )
        ejecutor = socket.gethostname()
        try:
            usuario = getpass.getuser()
        except Exception:  # sin entrada en /etc/passwd (contenedores)
            usuario = None
        filas = []
        for t in tablas:
            t_fin = t.fin or fin
            filas.append(
                (
                    self.run_id, self.proceso, self.capa, t.tabla, t.origen, t.destino, t.filas, t.bytes,
                    t.inicio, t_fin, (t_fin - t.inicio).total_seconds(), t.estado, t.error,
                    parametros, self.lanzado_por, ejecutor, usuario,
                )
            )
        return filas


def escribir_registro(con: duckdb.DuckDBPyConnection, destino: str, filas: list[tuple]) -> None:
    """Escribe las filas de una ejecución como un único Parquet con el esquema de CARGAS_COLUMNS."""
    ensure_parent_dir(destino)
    columnas = ", ".join(f"{nombre} {tipo}" for nombre, tipo in CARGAS_COLUMNS)
    marcadores = ", ".join("?" for _ in CARGAS_COLUMNS)
    con.execute(f"CREATE OR REPLACE TEMP TABLE registro_cargas ({columnas})")
    con.executemany(f"INSERT INTO registro_cargas VALUES ({marcadores})", filas)
    con.execute(f"COPY registro_cargas TO '{destino}' (FORMAT PARQUET)")
    con.execute("DROP TABLE registro_cargas")


@contextmanager
def registrar_carga(
    proceso: str,
    capa: str,
    layout: LakeLayout | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
    parametros: Mapping[str, object] | None = None,
    lanzado_por: str | None = None,
) -> Iterator[Ejecucion]:
    """Registra una ejecución de carga. Sin layout/con se resuelven del entorno al escribir."""
    ejecucion = Ejecucion(proceso, capa, parametros, lanzado_por or lanzado_por_defecto())
    try:
        yield ejecucion
    except BaseException as exc:
        ejecucion.marcar_error(exc)
        raise
    finally:
        _escribir_best_effort(ejecucion, layout, con)


def _escribir_best_effort(
    ejecucion: Ejecucion, layout: LakeLayout | None, con: duckdb.DuckDBPyConnection | None
) -> None:
    try:
        layout = layout or LakeLayout.from_env()
        con = con or connect(S3Settings.from_env() if layout.uses_s3 else None)
        destino = layout.cargas_file(ejecucion.run_id)
        escribir_registro(con, destino, ejecucion.filas_registro())
        logger.info("carga %s registrada en %s", ejecucion.run_id, destino)
    except Exception:
        logger.exception("no se pudo registrar la trazabilidad de la carga %s", ejecucion.run_id)


def leer_cargas(con: duckdb.DuckDBPyConnection, layout: LakeLayout, limit: int = 20) -> list[tuple]:
    """Últimas cargas registradas (más recientes primero)."""
    return con.execute(
        "SELECT run_id, proceso, capa, tabla, filas, inicio, round(duracion_s, 1), estado, lanzado_por, error "
        f"FROM read_parquet('{layout.cargas_glob()}', union_by_name = true) "
        "ORDER BY inicio DESC, tabla LIMIT ?",
        [limit],
    ).fetchall()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m raillytics.utils.cargas",
        description="Muestra las últimas cargas registradas en la trazabilidad del lake",
    )
    parser.add_argument("--limit", type=int, default=20, help="número de filas (por defecto 20)")
    args = parser.parse_args(argv)

    layout = LakeLayout.from_env()
    con = connect(S3Settings.from_env() if layout.uses_s3 else None)
    try:
        filas = leer_cargas(con, layout, args.limit)
    except duckdb.IOException:
        print(f"Sin cargas registradas en {layout.cargas_glob()}")
        return 0
    print(f"{'inicio (UTC)':<21}{'proceso':<18}{'capa':<8}{'tabla':<26}{'filas':>10}{'seg':>8}  {'estado':<7}{'lanzado por':<14}run_id")
    for run_id, proceso, capa, tabla, filas_n, inicio, dur, estado, lanzado_por, error in filas:
        print(
            f"{inicio:%Y-%m-%d %H:%M:%S}  {proceso:<18}{capa:<8}{(tabla or '-'):<26}"
            f"{(f'{filas_n:,}' if filas_n is not None else '-'):>10}{(dur if dur is not None else 0):>8.1f}  "
            f"{estado:<7}{lanzado_por:<14}{run_id}" + (f"\n{'':>21}{error}" if error else "")
        )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    raise SystemExit(main())
