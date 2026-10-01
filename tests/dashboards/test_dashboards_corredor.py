"""Todos los dashboards de negocio de Superset aplican al corredor AVE Madrid–Barcelona (AVE-MAD-BCN)."""
import json
from pathlib import Path

import duckdb
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


def _gold(tmp_path):
    """Tablas Gold mínimas con DOS líneas: la del corredor y otra que nunca debe aparecer."""
    con = duckdb.connect()
    def tabla(nombre, select):
        destino = tmp_path / nombre
        destino.mkdir(parents=True)
        con.execute(f"COPY ({select}) TO '{(destino / (nombre + '.parquet')).as_posix()}' (FORMAT PARQUET)")
    tabla("dim_linea", f"""SELECT * FROM (VALUES ('{CORREDOR}', 'AVE Madrid – Barcelona', 'AVE', 'Madrid', 'Barcelona'),
        ('C-MAD-C2', 'Cercanías Madrid C-2', 'Cercanías', 'Alcalá', 'Chamartín')) t(linea_id, nombre, tipo_tren, origen, destino)""")
    tabla("dim_estacion", """SELECT * FROM (VALUES ('MADPA', 'Madrid Puerta de Atocha', 'Madrid', 'Comunidad de Madrid', 40.4, -3.7),
        ('ALCHE', 'Alcalá de Henares', 'Madrid', 'Comunidad de Madrid', 40.5, -3.4)) t(estacion_id, nombre, provincia, comunidad, latitud, longitud)""")
    tabla("dim_fecha", """SELECT * FROM (VALUES (20261003, DATE '2026-10-03', 2026, 4, 10, 'octubre', 6, 'sábado', true, false, NULL::VARCHAR, 'otoño'),
        (20261005, DATE '2026-10-05', 2026, 4, 10, 'octubre', 1, 'lunes', false, false, NULL::VARCHAR, 'otoño'))
        t(fecha_id, fecha, anio, trimestre, mes, nombre_mes, dia_semana, nombre_dia, es_fin_de_semana, es_festivo, festivo_nombre, estacion_anio)""")
    tabla("fact_viajeros", f"""SELECT * FROM (VALUES (20261003, DATE '2026-10-03', 'MADPA', '{CORREDOR}', 1000, 15.0, 0.0, 'despejado'),
        (20261005, DATE '2026-10-05', 'MADPA', '{CORREDOR}', 3000, 14.0, 0.0, 'despejado'),
        (20261003, DATE '2026-10-03', 'ALCHE', 'C-MAD-C2', 9999, 15.0, 0.0, 'despejado'))
        t(fecha_id, fecha, estacion_id, linea_id, viajeros, temperatura_media, precipitacion_mm, condicion_meteo)""")
    tabla("fact_puntualidad", f"""SELECT * FROM (VALUES (20261003, DATE '2026-10-03', '{CORREDOR}', 'MADPA', 's1', TIMESTAMP '2026-10-03 08:00:00', TIMESTAMP '2026-10-03 08:03:00', 8, 3, 'realizado', false, true, 'despejado', 15.0, 0.0),
        (20261003, DATE '2026-10-03', 'C-MAD-C2', 'ALCHE', 's2', TIMESTAMP '2026-10-03 09:00:00', TIMESTAMP '2026-10-03 09:30:00', 9, 30, 'realizado', false, false, 'despejado', 15.0, 0.0))
        t(fecha_id, fecha, linea_id, estacion_id, servicio_id, hora_prevista, hora_real, hora, retraso_min, estado, cancelado, es_puntual, condicion_meteo, temperatura_media, precipitacion_mm)""")
    return con


@pytest.mark.parametrize("nombre", ("viajeros_diarios", "puntualidad_servicios"))
def test_los_datasets_de_hechos_solo_devuelven_el_corredor(tmp_path, nombre):
    con = _gold(tmp_path)
    sql = DATASETS[nombre]["sql"].replace("s3://raillytics-gold", tmp_path.as_posix())

    filas = con.execute(f"SELECT linea, tipo_tren FROM ({sql}) t").fetchall()

    assert filas and {f[0] for f in filas} == {"AVE Madrid – Barcelona"} and {f[1] for f in filas} == {"AVE"}


def test_el_dataset_de_demanda_no_cuenta_viajeros_de_otras_lineas_y_calcula_el_peso_del_fin_de_semana(tmp_path):
    con = _gold(tmp_path)
    sql = DATASETS["viajeros_diarios"]["sql"].replace("s3://raillytics-gold", tmp_path.as_posix())
    metricas = {m["metric_name"]: m["expression"] for m in DATASETS["viajeros_diarios"]["metrics"]}

    consulta = lambda e: con.execute(f"SELECT {e} FROM ({sql}) t").fetchone()[0]  # noqa: E731

    assert consulta(metricas["total_viajeros"]) == 4000  # sin los 9.999 de Cercanías
    assert consulta(metricas["pct_viajeros_fin_de_semana"]) == pytest.approx(25.0)  # 1.000 de 4.000 en sábado
    assert "n_lineas" not in metricas  # con una sola línea no tiene sentido contarlas
