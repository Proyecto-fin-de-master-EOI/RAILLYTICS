"""Todos los dashboards de negocio de Superset aplican al corredor AVE Madrid–Barcelona (AVE-MAD-BCN)."""
import json
from pathlib import Path

import pytest
import yaml

BASE = Path(__file__).resolve().parents[2] / "dashboards" / "superset" / "raillytics_gold"
CORREDOR = "AVE-MAD-BCN"
DATASETS = {p.stem: yaml.safe_load(p.read_text(encoding="utf-8")) for p in (BASE / "datasets" / "Raillytics_Gold_DuckDB").glob("*.yaml")}
GRAFICOS = {p.stem: yaml.safe_load(p.read_text(encoding="utf-8")) for p in (BASE / "charts").glob("*.yaml")}
DASHBOARDS = {p.stem: yaml.safe_load(p.read_text(encoding="utf-8")) for p in (BASE / "dashboards").glob("*.yaml")}
DE_NEGOCIO = ("demanda_ferroviaria", "puntualidad", "prediccion_demanda")  # trazabilidad_cargas habla del lake, no del corredor
DIMENSIONES_DE_VARIAS_LINEAS = {"linea", "tipo_tren", "linea_id"}


def _graficos_de(dashboard):
    uuids = {c["meta"]["uuid"] for c in DASHBOARDS[dashboard]["position"].values() if isinstance(c, dict) and c.get("type") == "CHART"}
    return {nombre: g for nombre, g in GRAFICOS.items() if g["uuid"] in uuids}


def _columnas_de(grafico):
    p = grafico["params"]
    groupby = p.get("groupby") or []
    columnas = set([groupby] if isinstance(groupby, str) else groupby)
    columnas |= set(p.get("all_columns") or []) | ({p["x_axis"]} if p.get("x_axis") else set())
    return columnas


@pytest.mark.parametrize("dashboard", DE_NEGOCIO)
def test_el_titulo_del_dashboard_dice_que_es_del_corredor(dashboard):
    titulo = DASHBOARDS[dashboard]["dashboard_title"]

    assert "Madrid" in titulo and "Barcelona" in titulo
    assert "AVE" in DASHBOARDS[dashboard]["description"] and "Madrid" in DASHBOARDS[dashboard]["description"]


@pytest.mark.parametrize("dashboard", ("demanda_ferroviaria", "puntualidad"))
def test_ningun_grafico_ni_filtro_agrupa_por_linea_o_tipo_de_tren(dashboard):
    for nombre, grafico in _graficos_de(dashboard).items():
        assert not (_columnas_de(grafico) & DIMENSIONES_DE_VARIAS_LINEAS), nombre
        assert "tipo de tren" not in grafico["slice_name"].lower() and "por línea" not in grafico["slice_name"].lower(), nombre
    for filtro in DASHBOARDS[dashboard]["metadata"]["native_filter_configuration"]:
        for destino in filtro["targets"]:
            assert destino.get("column", {}).get("name") not in DIMENSIONES_DE_VARIAS_LINEAS, filtro["name"]


@pytest.mark.parametrize("dashboard", ("demanda_ferroviaria", "puntualidad"))
def test_cada_grafico_del_dashboard_existe_y_usa_columnas_y_metricas_de_su_dataset(dashboard):
    por_uuid = {d["uuid"]: d for d in DATASETS.values()}
    colocados = [c["meta"]["uuid"] for c in DASHBOARDS[dashboard]["position"].values() if isinstance(c, dict) and c.get("type") == "CHART"]
    assert sorted(colocados) == sorted(g["uuid"] for g in _graficos_de(dashboard).values())  # ninguno huérfano ni inexistente
    for nombre, grafico in _graficos_de(dashboard).items():
        dataset = por_uuid[grafico["dataset_uuid"]]
        p = grafico["params"]
        metricas = set(p.get("metrics") or []) | ({p["metric"]} if isinstance(p.get("metric"), str) else set())
        assert metricas <= {m["metric_name"] for m in dataset["metrics"]}, (nombre, metricas)
        assert _columnas_de(grafico) <= {c["column_name"] for c in dataset["columns"]}, nombre


@pytest.mark.parametrize("nombre", ("viajeros_diarios", "puntualidad_servicios"))
def test_los_datasets_de_hechos_solo_devuelven_el_corredor(gold, nombre):
    filas = gold(DATASETS[nombre]["sql"], "linea, tipo_tren")

    assert filas and {f[0] for f in filas} == {"AVE Madrid – Barcelona"} and {f[1] for f in filas} == {"AVE"}


def test_el_dataset_de_demanda_no_cuenta_viajeros_de_otras_lineas_y_calcula_el_peso_del_fin_de_semana(gold):
    sql = DATASETS["viajeros_diarios"]["sql"]
    metricas = {m["metric_name"]: m["expression"] for m in DATASETS["viajeros_diarios"]["metrics"]}

    consulta = lambda e: gold(sql, e)[0][0]  # noqa: E731

    assert consulta(metricas["total_viajeros"]) == 4000  # sin los 9.999 de Cercanías
    assert consulta(metricas["pct_viajeros_fin_de_semana"]) == pytest.approx(25.0)  # 1.000 de 4.000 en sábado
    assert "n_lineas" not in metricas  # con una sola línea no tiene sentido contarlas
