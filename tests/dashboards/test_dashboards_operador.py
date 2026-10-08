"""Los dashboards de demanda y puntualidad y sus datos se pueden ver por operador (Renfe, Iryo, Ouigo, Avlo)."""
import pytest

from test_dashboards_corredor import DASHBOARDS, DATASETS, GRAFICOS, _graficos_de  # mismo directorio: pytest lo añade al sys.path

COLUMNAS_DEL_OPERADOR = {"operador", "operador_empresa", "operador_segmento"}
GRAFICOS_POR_OPERADOR = {
    "demanda_ferroviaria": {"reparto_operador", "evolucion_viajeros_operador", "resumen_operador"},
    "puntualidad": {"puntualidad_operador", "evolucion_puntualidad_operador"},
}
DATASET_DE = {"demanda_ferroviaria": "viajeros_diarios", "puntualidad": "puntualidad_servicios"}


@pytest.mark.parametrize("nombre", ("viajeros_diarios", "puntualidad_servicios"))
def test_los_datasets_declaran_las_columnas_del_operador(nombre):
    declaradas = {c["column_name"] for c in DATASETS[nombre]["columns"]}

    assert COLUMNAS_DEL_OPERADOR <= declaradas


@pytest.mark.parametrize("nombre", ("viajeros_diarios", "puntualidad_servicios"))
def test_los_datasets_traen_el_operador_con_su_empresa_y_segmento(gold, nombre):
    filas = gold(DATASETS[nombre]["sql"], "DISTINCT operador, operador_empresa, operador_segmento")

    assert set(filas) == {("Renfe", "Renfe Viajeros", "Alta velocidad"), ("Ouigo", "SNCF Voyageurs España", "Low cost")}


def test_los_viajeros_por_operador_suman_el_total_del_corredor_sin_duplicar_filas_en_el_join(gold):
    ds = DATASETS["viajeros_diarios"]
    total = {m["metric_name"]: m["expression"] for m in ds["metrics"]}["total_viajeros"]

    por_operador = dict(gold(ds["sql"], f"operador, {total}", "GROUP BY operador"))

    assert por_operador == {"Renfe": 2600, "Ouigo": 1400}  # sin los 9.999 de Cercanías
    assert sum(por_operador.values()) == gold(ds["sql"], total)[0][0] == 4000


def test_la_puntualidad_se_calcula_por_operador(gold):
    ds = DATASETS["puntualidad_servicios"]
    metricas = {m["metric_name"]: m["expression"] for m in ds["metrics"]}

    por_operador = {op: (n, pct) for op, n, pct in gold(ds["sql"], f"operador, {metricas['n_servicios']}, {metricas['pct_puntuales']}", "GROUP BY operador")}

    assert por_operador == {"Renfe": (1, pytest.approx(100.0)), "Ouigo": (1, pytest.approx(0.0))}


@pytest.mark.parametrize("dashboard", GRAFICOS_POR_OPERADOR)
def test_cada_dashboard_tiene_sus_graficos_por_operador_colocados_en_la_rejilla(dashboard):
    en_el_dashboard = set(_graficos_de(dashboard))

    assert GRAFICOS_POR_OPERADOR[dashboard] <= en_el_dashboard


@pytest.mark.parametrize("dashboard", GRAFICOS_POR_OPERADOR)
def test_los_graficos_por_operador_usan_el_dataset_de_su_dashboard_y_la_columna_operador(dashboard):
    uuid_dataset = DATASETS[DATASET_DE[dashboard]]["uuid"]

    for nombre in GRAFICOS_POR_OPERADOR[dashboard]:
        grafico = GRAFICOS[nombre]
        p = grafico["params"]
        columnas = set([p["groupby"]] if isinstance(p.get("groupby"), str) else p.get("groupby") or []) | set(p.get("all_columns") or [])
        assert grafico["dataset_uuid"] == uuid_dataset, nombre
        assert "operador" in columnas, nombre


@pytest.mark.parametrize("dashboard", GRAFICOS_POR_OPERADOR)
def test_cada_dashboard_filtra_por_operador_con_un_filtro_nativo(dashboard):
    filtros = DASHBOARDS[dashboard]["metadata"]["native_filter_configuration"]
    por_operador = [f for f in filtros if f["targets"][0].get("column", {}).get("name") == "operador"]

    assert len(por_operador) == 1
    assert por_operador[0]["name"] == "Operador" and por_operador[0]["filterType"] == "filter_select"
    assert por_operador[0]["targets"][0]["datasetUuid"] == DATASETS[DATASET_DE[dashboard]]["uuid"]
    assert por_operador[0]["scope"] == {"rootPath": ["ROOT_ID"], "excluded": []}


def test_el_resumen_por_operador_muestra_empresa_y_segmento():
    columnas = GRAFICOS["resumen_operador"]["params"]["groupby"]

    assert columnas == ["operador", "operador_empresa", "operador_segmento"]
