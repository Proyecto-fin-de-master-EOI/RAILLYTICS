"""El SQL de cada dataset de Superset se sostiene solo: no cita CTE que no define.

Por qué existe este fichero: el 2026-10-14, al colapsar los dos escaneos de la trazabilidad para
arreglar un timeout, el dataset `kpi_plan` perdió dos CTE (`dtc` y `franja`) y se quedó citándolos.
La consulta entera dejó de ejecutarse —no un KPI, LA TABLA— y nadie lo vio en cuatro días, porque los
tests miraban qué columnas declara cada dataset pero no si su SQL se puede resolver.

Esto no ejecuta nada contra el lago (los tests corren sin MinIO): comprueba que toda tabla citada en
un FROM o un JOIN sea o un CTE definido en la propia consulta, o una lectura de ficheros, o un alias
de los que el propio SQL crea. Es barato y caza exactamente la clase de fallo que se nos escapó.
"""
import re
from pathlib import Path

import pytest
import yaml

BASE = Path(__file__).resolve().parents[2] / "dashboards" / "superset" / "raillytics_gold" / "datasets"
DATASETS = {p.stem: yaml.safe_load(p.read_text(encoding="utf-8")) for p in BASE.rglob("*.yaml")}
CON_SQL = sorted(nombre for nombre, d in DATASETS.items() if (d or {}).get("sql"))

# Lo que puede aparecer tras FROM/JOIN sin ser un CTE: funciones de tabla y palabras de la sintaxis.
FUNCIONES_DE_TABLA = {"read_parquet", "read_csv", "read_json", "glob", "values", "unnest",
                      "generate_series", "range", "lateral", "select"}

# `nombre AS (`, `nombre(col, col) AS (` o `nombre AS MATERIALIZED (`, detrás de WITH o de una coma.
# El MATERIALIZED importa: `kpi_plan` lo usa para leer la trazabilidad una sola vez.
DEFINICION = re.compile(
    r"(?:^|[,\s])([a-z_][a-z0-9_]*)\s*(?:\([^()]*\))?\s+AS\s*(?:(?:NOT\s+)?MATERIALIZED\s*)?\(",
    re.I | re.M,
)
REFERENCIA = re.compile(r"\b(?:FROM|JOIN)\s+([a-z_][a-z0-9_]*)\b", re.I)


def _sin_literales(sql: str) -> str:
    """Quita las cadenas, que llevan dentro palabras como «from» en las notas de los KPI."""
    return re.sub(r"'(?:[^']|'')*'", "''", sql)


@pytest.mark.parametrize("nombre", CON_SQL)
def test_el_sql_no_cita_ctes_que_no_define(nombre):
    sql = _sin_literales(DATASETS[nombre]["sql"])
    definidos = {m.lower() for m in DEFINICION.findall(sql)}
    referidos = {m.lower() for m in REFERENCIA.findall(sql)}

    huerfanos = referidos - definidos - FUNCIONES_DE_TABLA
    assert huerfanos == set(), f"{nombre} cita sin definir: {', '.join(sorted(huerfanos))}"


def test_el_dataset_de_kpi_define_los_ctes_de_los_que_cuelgan_los_kpi():
    """Los dos que se perdieron, con nombre y apellido, para que la regresión no vuelva en silencio."""
    sql = DATASETS["kpi_plan"]["sql"]

    for cte in ("dtc", "franja"):
        assert f"{cte} AS (" in sql, f"el CTE {cte} ha vuelto a desaparecer de kpi_plan"


def test_ningun_kpi_desaparece_de_la_tabla_cuando_no_se_puede_medir():
    """Un KPI sin valor se queda EN BLANCO; si se filtra, no se distingue «hoy no aplica» de «roto»."""
    sql = DATASETS["kpi_plan"]["sql"]

    assert "WHERE valor IS NOT NULL" not in sql
