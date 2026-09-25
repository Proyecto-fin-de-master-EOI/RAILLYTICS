from datetime import date
from pathlib import Path

import duckdb
import pytest

from raillytics.gold.build import GOLD_TABLES, PUNTUALIDAD_UMBRAL_MIN, build_gold, write_catalog
from raillytics.utils.lake import LakeLayout, connect
from raillytics.procesamiento.silver_sample import generate_silver_sample, write_silver_sample

# Tres semanas que incluyen Viernes Santo (18/4/2025) y el 1 de mayo.
START, END = date(2025, 4, 14), date(2025, 5, 4)


@pytest.fixture(scope="module")
def gold(tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    layout = LakeLayout(silver_root=(root / "silver").as_posix(), gold_root=(root / "gold").as_posix())
    sample = generate_silver_sample(START, END, seed=7)
    write_silver_sample(connect(), layout, sample)
    counts = build_gold(connect(), layout)
    return layout, sample, counts


def query(layout: LakeLayout, sql: str) -> list[tuple]:
    """Ejecuta sql leyendo las tablas Gold desde su Parquet: {dim_fecha} -> read_parquet(...)."""
    sources = {table: f"read_parquet('{layout.gold_glob(table)}')" for table in GOLD_TABLES}
    return duckdb.connect().execute(sql.format(**sources)).fetchall()


def test_build_writes_every_gold_table_as_parquet(gold):
    layout, _, counts = gold

    assert tuple(counts) == GOLD_TABLES
    for table in GOLD_TABLES:
        assert Path(layout.gold_file(table)).is_file()
        assert query(layout, f"SELECT count(*) FROM {{{table}}}") == [(counts[table],)]
        assert counts[table] > 0


def test_dim_fecha_has_one_row_per_day_with_calendar_attributes(gold):
    layout, _, counts = gold

    assert counts["dim_fecha"] == (END - START).days + 1
    assert query(layout, "SELECT count(DISTINCT fecha_id) FROM {dim_fecha}") == [(counts["dim_fecha"],)]

    viernes_santo = query(
        layout,
        "SELECT fecha_id, dia_semana, nombre_dia, es_fin_de_semana, es_festivo, festivo_nombre, estacion_anio "
        "FROM {dim_fecha} WHERE fecha = DATE '2025-04-18'",
    )
    assert viernes_santo == [(20250418, 5, "viernes", False, True, "Viernes Santo", "primavera")]
    assert query(layout, "SELECT es_festivo, es_fin_de_semana FROM {dim_fecha} WHERE fecha = DATE '2025-04-20'") == [
        (False, True)
    ]
    assert query(layout, "SELECT festivo_nombre FROM {dim_fecha} WHERE fecha = DATE '2025-05-01'") == [
        ("Fiesta del Trabajo",)
    ]


def test_fact_viajeros_keeps_silver_grain_and_resolves_to_dimensions(gold):
    layout, sample, counts = gold

    assert counts["fact_viajeros"] == len(sample.viajeros)
    assert query(layout, "SELECT sum(viajeros) FROM {fact_viajeros}") == [(int(sample.viajeros.viajeros.sum()),)]

    orphans = query(
        layout,
        "SELECT "
        "  count(*) FILTER (WHERE d.fecha_id IS NULL), "
        "  count(*) FILTER (WHERE e.estacion_id IS NULL), "
        "  count(*) FILTER (WHERE l.linea_id IS NULL) "
        "FROM {fact_viajeros} f "
        "LEFT JOIN {dim_fecha} d USING (fecha_id) "
        "LEFT JOIN {dim_estacion} e USING (estacion_id) "
        "LEFT JOIN {dim_linea} l USING (linea_id)",
    )
    assert orphans == [(0, 0, 0)]


def test_fact_puntualidad_derives_flags_from_delay_and_state(gold):
    layout, sample, counts = gold

    assert counts["fact_puntualidad"] == len(sample.puntualidad)

    inconsistent = query(
        layout,
        "SELECT count(*) FROM {fact_puntualidad} "
        f"WHERE es_puntual <> (estado <> 'cancelado' AND retraso_min <= {PUNTUALIDAD_UMBRAL_MIN}) "
        "   OR cancelado <> (estado = 'cancelado') "
        "   OR (cancelado AND (retraso_min IS NOT NULL OR hora_real IS NOT NULL)) "
        "   OR hora <> hour(hora_prevista)",
    )
    assert inconsistent == [(0,)]
    # La muestra contiene los tres casos que distinguen los KPIs de puntualidad.
    ((puntuales, retrasados, cancelados),) = query(
        layout,
        "SELECT count(*) FILTER (WHERE es_puntual), "
        "       count(*) FILTER (WHERE NOT es_puntual AND NOT cancelado), "
        "       count(*) FILTER (WHERE cancelado) "
        "FROM {fact_puntualidad}",
    )
    assert puntuales > 0 and retrasados > 0 and cancelados > 0


def test_build_is_idempotent_and_overwrites_previous_parquet(gold):
    layout, _, counts = gold

    assert build_gold(connect(), layout) == counts
    assert query(layout, "SELECT count(*) FROM {fact_viajeros}") == [(counts["fact_viajeros"],)]


def test_silver_and_gold_loads_are_traced(gold):
    layout, sample, counts = gold

    trazas = duckdb.connect().execute(
        "SELECT proceso, tabla, filas, estado, parametros FROM read_parquet(?) "
        "WHERE run_id IN (SELECT run_id FROM read_parquet(?) QUALIFY row_number() OVER (PARTITION BY proceso ORDER BY inicio DESC) = 1) "
        "ORDER BY proceso, tabla",
        [layout.cargas_glob(), layout.cargas_glob()],
    ).fetchall()

    assert {(proceso, tabla, filas, estado) for proceso, tabla, filas, estado, _ in trazas} == {
        ("gold_build", table, counts[table], "ok") for table in GOLD_TABLES
    } | {
        ("silver_sample", "viajeros_enriquecidos", len(sample.viajeros), "ok"),
        ("silver_sample", "puntualidad_enriquecida", len(sample.puntualidad), "ok"),
    }
    parametros = {proceso: params for proceso, *_, params in trazas}
    assert f'"seed": {sample.seed}' in parametros["silver_sample"]
    assert f'"umbral_puntualidad_min": {PUNTUALIDAD_UMBRAL_MIN}' in parametros["gold_build"]


def test_write_catalog_exposes_gold_tables_as_views(gold, tmp_path):
    layout, _, counts = gold
    catalog = tmp_path / "raillytics_gold.duckdb"

    write_catalog(catalog, layout, s3=None)
    write_catalog(catalog, layout, s3=None)  # regenerar sobre el fichero anterior también vale

    con = duckdb.connect(str(catalog), read_only=True)
    assert con.execute("SELECT count(*) FROM fact_puntualidad").fetchone() == (counts["fact_puntualidad"],)
    views = {row[0] for row in con.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall()}
    assert views == set(GOLD_TABLES)
