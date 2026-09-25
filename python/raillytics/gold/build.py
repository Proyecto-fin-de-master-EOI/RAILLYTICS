"""Construye la capa Gold (modelo dimensional) con DuckDB.

Lee las tablas Silver del lake, ejecuta el SQL de cada tabla del modelo
(sql/<tabla>.sql, SQL estándar de DuckDB) y escribe el resultado como Parquet
en el bucket Gold, una tabla por prefijo:

    s3://raillytics-gold/dim_fecha/dim_fecha.parquet
    s3://raillytics-gold/dim_estacion/...  dim_linea/  fact_viajeros/  fact_puntualidad/

Es el equivalente local del paso "dbt -> Snowflake" del diseño: mismo modelo
(Dim_Fecha, Dim_Estacion, Dim_Linea, Fact_Viajeros, Fact_Puntualidad), pero
el motor es DuckDB y el almacenamiento Parquet en MinIO, que es lo que
consulta Superset (dashboards/superset/). Cada ejecución reconstruye las
tablas completas: con el volumen de este proyecto es más simple y seguro que
una carga incremental.

Uso:  python -m raillytics.gold.build [--catalog data/gold/raillytics_gold.duckdb]
"""
from __future__ import annotations

import argparse
import logging
from collections.abc import Iterable
from pathlib import Path

import duckdb

from raillytics.gold.lake import LakeLayout, S3Settings, connect, ensure_parent_dir

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).with_name("sql")

# Tablas Silver de entrada. El contrato de columnas está documentado (y, de
# momento, generado) en raillytics/procesamiento/silver_sample.py.
SILVER_TABLES = ("viajeros_enriquecidos", "puntualidad_enriquecida")

# Orden de construcción: dimensiones antes que hechos, para que un fallo en
# una dimensión aborte antes de escribir hechos que no podrían resolverse.
GOLD_TABLES = ("dim_fecha", "dim_estacion", "dim_linea", "fact_viajeros", "fact_puntualidad")

# Un servicio cuenta como puntual si llega con este retraso o menos (minutos).
# fact_puntualidad.sql lo lee con getvariable('umbral_puntualidad_min').
PUNTUALIDAD_UMBRAL_MIN = 5


def register_silver(con: duckdb.DuckDBPyConnection, layout: LakeLayout) -> None:
    """Expone cada tabla Silver como vista silver_<tabla> sobre su Parquet."""
    for table in SILVER_TABLES:
        con.execute(
            f"CREATE OR REPLACE VIEW silver_{table} AS SELECT * FROM read_parquet('{layout.silver_glob(table)}')"
        )


def read_model_sql(table: str) -> str:
    return (SQL_DIR / f"{table}.sql").read_text(encoding="utf-8").strip().rstrip(";")


def build_gold(
    con: duckdb.DuckDBPyConnection,
    layout: LakeLayout,
    tables: Iterable[str] = GOLD_TABLES,
) -> dict[str, int]:
    """Construye y escribe las tablas Gold. Devuelve el número de filas de cada una."""
    register_silver(con, layout)
    con.execute(f"SET VARIABLE umbral_puntualidad_min = {PUNTUALIDAD_UMBRAL_MIN}")

    counts: dict[str, int] = {}
    for table in tables:
        con.execute(f"CREATE OR REPLACE TABLE {table} AS {read_model_sql(table)}")
        dest = layout.gold_file(table)
        ensure_parent_dir(dest)
        con.execute(f"COPY {table} TO '{dest}' (FORMAT PARQUET)")
        counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        logger.info("%s: %d filas -> %s", table, counts[table], dest)
    return counts


def write_catalog(path: Path, layout: LakeLayout, s3: S3Settings | None) -> None:
    """(Re)crea un fichero DuckDB con vistas sobre el Parquet de Gold.

    Pensado para notebooks o clientes SQL (DBeaver): no contiene datos, solo
    vistas, así que se puede borrar y regenerar sin coste. Al abrirlo hay que
    volver a registrar las credenciales de MinIO, p. ej.:
        lake.connect(S3Settings.from_env(), database=str(path), read_only=True)
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    for stale in (path, path.with_name(path.name + ".wal")):
        stale.unlink(missing_ok=True)

    con = connect(s3, database=str(path))
    try:
        for table in GOLD_TABLES:
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{layout.gold_glob(table)}')")
    finally:
        con.close()
    logger.info("catálogo DuckDB con vistas sobre Gold: %s", path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m raillytics.gold.build",
        description="Construye la capa Gold (DuckDB) a partir de Silver y la deja en Parquet",
    )
    parser.add_argument(
        "--catalog",
        type=Path,
        metavar="FICHERO.duckdb",
        help="además, (re)crea un fichero DuckDB local con vistas sobre Gold (para notebooks)",
    )
    args = parser.parse_args(argv)

    layout = LakeLayout.from_env()
    s3 = S3Settings.from_env() if layout.uses_s3 else None
    con = connect(s3)

    print(f"Silver: {layout.silver_root}  ->  Gold: {layout.gold_root}")
    counts = build_gold(con, layout)
    for table, rows in counts.items():
        print(f"  {table:<18}{rows:>10,} filas")
    if args.catalog:
        write_catalog(args.catalog, layout, s3)
        print(f"Catálogo DuckDB con vistas sobre Gold: {args.catalog}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    raise SystemExit(main())
