"""Dashboard «Mercado del corredor»: datos reales de la CNMC (viajeros, cuota, oferta, precio y backtest del nivel de la predicción)."""
import pytest

from test_dashboards_corredor import DASHBOARDS, DATASETS, _graficos_de

MERCADO = ("mercado_trimestral", "precio_medio_trimestral", "precio_medio_mensual", "backtest_nivel")
GRAFICOS_ESPERADOS = {
    "kpi_mercado_viajeros", "kpi_mercado_yoy", "kpi_mercado_cuota_renfe", "kpi_mercado_precio",
    "mercado_viajeros_operador", "mercado_cuota_operador", "mercado_viajeros_plaza", "mercado_tren_km", "mercado_ingresos",
    "precio_mensual_operador", "backtest_previsto_real", "backtest_error", "kpi_backtest_mape", "mercado_resumen",
}


def test_los_cuatro_datasets_ejecutan_su_sql_sobre_el_gold(gold):
    for nombre in MERCADO:
        assert gold(DATASETS[nombre]["sql"], "count(*)")[0][0] > 0, nombre


def test_la_cuota_de_los_operadores_suma_uno_cada_trimestre_y_el_total_del_corredor_no_tiene_cuota(gold):
    sql = DATASETS["mercado_trimestral"]["sql"]

    sumas = gold(sql, "trimestre_etiqueta, round(sum(cuota_viajeros), 6)", "GROUP BY trimestre_etiqueta ORDER BY 1")

    assert len(sumas) == 7 and all(suma == 1.0 for _, suma in sumas)
    assert gold(sql, "count(*)", "WHERE es_total AND cuota_viajeros IS NOT NULL")[0][0] == 0


def test_los_operadores_se_etiquetan_sin_inventar_a_avlo_en_la_demanda(gold):
    demanda = {r[0] for r in gold(DATASETS["mercado_trimestral"]["sql"], "DISTINCT operador")}
    precios = {r[0] for r in gold(DATASETS["precio_medio_mensual"]["sql"], "DISTINCT operador")}

    assert demanda == {"Renfe (incl. Avlo)", "Ouigo", "Total corredor"}   # la CNMC no separa Avlo de Renfe en la demanda
    assert precios == {"Renfe (AVE)", "Avlo", "Total corredor"}           # en los precios sí


def test_el_ultimo_trimestre_y_la_variacion_interanual_del_corredor(gold):
    fila = gold(DATASETS["mercado_trimestral"]["sql"],
                "trimestre_etiqueta, round(variacion_interanual_corredor, 4), viajeros_corredor", "WHERE es_ultimo_trimestre AND NOT es_total LIMIT 1")[0]

    assert fila == ("2026-T2", pytest.approx(-0.1793, abs=1e-4), 1190)   # 1.190 frente a 1.450 del mismo trimestre de 2025


def test_el_backtest_usa_la_regla_de_nivel_de_la_prediccion(gold):
    filas = gold(DATASETS["backtest_nivel"]["sql"], "trimestre_etiqueta, real_total, previsto_total, round(error_absoluto, 4), ultimos_8")

    # previsto(T) = real(T-4) × real(T-2) / real(T-6) = 1.450 × 1.330 / 1.200 ≈ 1.607 frente a 1.190 reales
    assert filas == [("2026-T2", 1190, 1607, pytest.approx(0.3504, abs=1e-4), True)]


def test_el_dashboard_tiene_los_14_graficos_colocados_y_cada_uno_usa_su_dataset():
    graficos = _graficos_de("mercado_corredor")

    assert set(graficos) == GRAFICOS_ESPERADOS
    uuids_mercado = {DATASETS[d]["uuid"] for d in MERCADO}
    assert {g["dataset_uuid"] for g in graficos.values()} <= uuids_mercado


def test_el_dashboard_filtra_por_fechas_y_por_operador_y_dice_que_son_datos_reales_de_la_cnmc():
    dashboard = DASHBOARDS["mercado_corredor"]
    filtros = {f["name"]: f for f in dashboard["metadata"]["native_filter_configuration"]}

    assert set(filtros) == {"Rango de fechas", "Operador"}
    assert filtros["Operador"]["targets"][0]["datasetUuid"] == DATASETS["mercado_trimestral"]["uuid"]
    assert filtros["Operador"]["targets"][0]["column"]["name"] == "operador"
    assert "CNMC" in dashboard["description"] and "Avlo" in dashboard["description"] and "no es ocupación" in dashboard["description"]
    assert dashboard["slug"] == "mercado-corredor" and "Madrid–Barcelona" in dashboard["dashboard_title"]
