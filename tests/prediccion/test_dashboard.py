"""El dashboard de Superset «Predicción de demanda» (como código en dashboards/superset/) debe encajar con lo que publica la predicción."""
import json
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest
import yaml

from raillytics.prediccion.calendario import construir_calendario
from raillytics.prediccion.normalizar import IndiceDia, normalizar_indices, repartir
from raillytics.prediccion.publicacion import COLUMNAS_GOLD, GOLD_TABLA, construir_gold, publicar_gold
from raillytics.prediccion.salida import construir_dataframe, resumen_coherencia
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.lake import LakeLayout, connect

BASE = Path(__file__).resolve().parents[2] / "dashboards" / "superset" / "raillytics_gold"
CARGAR = lambda ruta: yaml.safe_load((BASE / ruta).read_text(encoding="utf-8"))  # noqa: E731
DATASET = CARGAR("datasets/Raillytics_Gold_DuckDB/prediccion_demanda.yaml")
GRAFICOS = {p.stem: CARGAR(f"charts/{p.name}") for p in sorted((BASE / "charts").glob("pred_*.yaml"))}
DASHBOARD = CARGAR("dashboards/prediccion_demanda.yaml")
T4 = Trimestre(2026, 4)


def test_hay_dataset_graficos_y_dashboard():
    assert DATASET["table_name"] == "prediccion_demanda" and DASHBOARD["slug"] == "prediccion-demanda"
    assert len(GRAFICOS) >= 8 and DASHBOARD["published"] is True


def test_cada_grafico_apunta_al_dataset_y_todos_los_uuid_son_unicos():
    assert all(g["dataset_uuid"] == DATASET["uuid"] for g in GRAFICOS.values())
    todos = [CARGAR(p.relative_to(BASE))["uuid"] for p in BASE.rglob("*.yaml") if p.name != "metadata.yaml"]
    assert len(todos) == len(set(todos))  # sin choques con los dashboards, gráficos y datasets que ya existían


def test_el_dashboard_coloca_cada_grafico_exactamente_una_vez():
    posicion = DASHBOARD["position"]
    colocados = [c["meta"]["uuid"] for c in posicion.values() if isinstance(c, dict) and c.get("type") == "CHART"]

    assert sorted(colocados) == sorted(g["uuid"] for g in GRAFICOS.values())
    nombres = {c["meta"]["sliceName"] for c in posicion.values() if isinstance(c, dict) and c.get("type") == "CHART"}
    assert nombres == {g["slice_name"] for g in GRAFICOS.values()}
    # cada gráfico cuelga de una fila que existe y esa fila de la rejilla
    for componente in posicion.values():
        if isinstance(componente, dict) and componente.get("type") == "CHART":
            fila = posicion[componente["parents"][-1]]
            assert componente["id"] in fila["children"] and fila["id"] in posicion["GRID_ID"]["children"]


def test_los_graficos_solo_usan_columnas_y_metricas_que_existen_en_el_dataset():
    columnas = {c["column_name"] for c in DATASET["columns"]}
    metricas = {m["metric_name"] for m in DATASET["metrics"]}
    for nombre, grafico in GRAFICOS.items():
        p = grafico["params"]
        usadas_metricas = set(p.get("metrics", [])) | ({p["metric"]} if "metric" in p else set())
        usadas_columnas = set(p.get("groupby", [])) | set(p.get("all_columns", [])) | ({p["x_axis"]} if p.get("x_axis") else set())
        assert usadas_metricas <= metricas, (nombre, usadas_metricas - metricas)
        assert usadas_columnas <= columnas, (nombre, usadas_columnas - columnas)


def test_los_filtros_nativos_apuntan_a_columnas_del_dataset():
    columnas = {c["column_name"] for c in DATASET["columns"]}
    filtros = DASHBOARD["metadata"]["native_filter_configuration"]

    assert filtros
    for filtro in filtros:
        for destino in filtro["targets"]:
            if destino:
                assert destino["datasetUuid"] == DATASET["uuid"] and destino["column"]["name"] in columnas


def _calendario():
    return construir_calendario(
        T4,
        # Inmaculada en martes: el lunes 7 es puente y hay víspera, puente y regreso que medir.
        pd.DataFrame([(date(2026, 12, 8), "Inmaculada Concepción"), (date(2026, 12, 25), "Navidad")], columns=["fecha", "nombre"]),
        pd.DataFrame([(date(2026, 11, 29), "Partido de liga", "MAD")], columns=["fecha", "descripcion", "ciudad"]),
        pd.DataFrame([], columns=["fecha", "ciudad", "temperatura_media", "precipitacion_mm"]),
    )


def _publicar(layout, run_id, version, sinteticos, n):
    valores = [1.0 + 0.1 * ((i + n) % 3) for i in range(92)]
    indices = [IndiceDia(d, v, f"motivo {d.day}") for d, v in zip(T4.dias(), valores)]
    df = construir_dataframe(
        T4.dias(), indices, repartir(valores, 1_320_000), normalizar_indices(valores),
        trimestre=T4, modelo="mistral-nemo", version_prompt=version, run_id=run_id, generado_en=datetime(2026, 10, 1, 16, 51 + n),
    )
    gold = construir_gold(df, _calendario(), total_esperado=1_320_000, datos_sinteticos=sinteticos)
    publicar_gold(connect(), layout, gold)
    return gold


@pytest.fixture(scope="module")
def vista(tmp_path_factory):
    """El dataset de Superset evaluado en DuckDB sobre el Parquet que escribe publicar_gold (dos ejecuciones)."""
    tmp = tmp_path_factory.mktemp("lake")
    layout = LakeLayout(silver_root=(tmp / "silver").as_posix(), gold_root=(tmp / "gold").as_posix())
    for n, (run_id, version, sinteticos) in enumerate([("run-1", "demanda_v1", True), ("run-2", "demanda_v2", False)]):
        _publicar(layout, run_id, version, sinteticos, n)
    sql = DATASET["sql"].replace("s3://raillytics-gold", layout.gold_root)
    return duckdb.connect(), sql


def test_la_sql_del_dataset_devuelve_exactamente_las_columnas_declaradas(vista):
    con, sql = vista

    resultado = con.execute(f"SELECT * FROM ({sql}) t")

    assert [c[0] for c in resultado.description] == [c["column_name"] for c in DATASET["columns"]]
    assert len(resultado.fetchall()) == 184  # 92 días × 2 ejecuciones


def test_la_sql_del_dataset_lee_la_tabla_que_publica_la_prediccion_y_usa_todas_sus_columnas():
    assert f"/{GOLD_TABLA}/*.parquet" in DATASET["sql"]
    for columna in COLUMNAS_GOLD:
        assert columna in DATASET["sql"], columna


def test_cada_metrica_del_dataset_se_calcula(vista):
    con, sql = vista

    for metrica in DATASET["metrics"]:
        valor = con.execute(f"SELECT {metrica['expression']} FROM ({sql}) t").fetchone()[0]
        assert valor is not None, metrica["metric_name"]


def test_las_metricas_y_columnas_clave_dan_los_valores_esperados(vista):
    con, sql = vista
    consulta = lambda expresion: con.execute(f"SELECT {expresion} FROM ({sql}) t").fetchone()[0]  # noqa: E731
    metricas = {m["metric_name"]: m["expression"] for m in DATASET["metrics"]}

    assert consulta(metricas["n_ejecuciones"]) == 2
    assert consulta(metricas["viajeros_previstos"]) == 2 * 1_320_000
    assert consulta(metricas["dias_festivo_o_evento"]) == 6  # 2 ejecuciones × (Inmaculada + Navidad + el partido)
    tipos = dict(con.execute(f"SELECT tipo_dia, count(*) FROM ({sql}) t WHERE run_id = 'run-1' GROUP BY 1").fetchall())
    assert tipos["Festivo"] == 2 and tipos["Evento"] == 1 and tipos["Fin de semana"] > 20 and tipos["Laborable"] > 40
    assert {r[0] for r in con.execute(f"SELECT DISTINCT datos FROM ({sql}) t").fetchall()} == {"Sintéticos", "Reales"}
    # solo una ejecución por trimestre es «la última», y es la más reciente
    assert con.execute(f"SELECT DISTINCT run_id FROM ({sql}) t WHERE es_ultima").fetchall() == [("run-2",)]
    # el orden de los días de la semana de las etiquetas es lun..dom
    assert [r[0] for r in con.execute(f"SELECT DISTINCT dia_semana_etiqueta FROM ({sql}) t ORDER BY 1").fetchall()][:2] == ["1 · lun", "2 · mar"]


@pytest.mark.parametrize("run_id", ["run-1", "run-2"])
def test_los_excesos_del_dashboard_son_los_del_resumen_que_imprime_cada_ejecucion(vista, run_id):
    con, sql = vista
    filas = con.execute(f"SELECT strftime(fecha, '%Y-%m-%d') AS fecha, indice FROM ({sql}) t WHERE run_id = '{run_id}'").df()
    esperado = resumen_coherencia(filas, _calendario())

    for metrica in DATASET["metrics"]:
        if metrica["metric_name"].startswith("exceso_"):
            valor = con.execute(f"SELECT {metrica['expression']} FROM ({sql}) t WHERE run_id = '{run_id}'").fetchone()[0]
            assert valor == pytest.approx(esperado[metrica["metric_name"]]), metrica["metric_name"]


def test_la_sql_del_dataset_admite_ejecuciones_publicadas_antes_de_las_columnas_de_contexto(tmp_path):
    layout = LakeLayout(silver_root=(tmp_path / "silver").as_posix(), gold_root=(tmp_path / "gold").as_posix())
    gold = _publicar(layout, "run-nuevo", "demanda_v3", True, 0)
    nuevas = ["vispera", "puente", "regreso", "junto_a_evento", "exceso"]
    antigua = (tmp_path / "gold" / GOLD_TABLA / "run-antiguo.parquet").as_posix()
    (tmp_path / "gold" / GOLD_TABLA / "run-nuevo.parquet").unlink()
    con = duckdb.connect()
    sql = DATASET["sql"].replace("s3://raillytics-gold", layout.gold_root)
    con.register("antigua", gold.drop(columns=nuevas).assign(run_id="run-antiguo"))
    con.execute(f"COPY (SELECT * REPLACE (CAST(fecha AS DATE) AS fecha) FROM antigua) TO '{antigua}' (FORMAT PARQUET)")

    solo_antiguas = con.execute(f"SELECT count(*), count(exceso), count(vispera) FROM ({sql}) t").fetchone()
    _publicar(layout, "run-nuevo", "demanda_v3", True, 0)
    por_ejecucion = dict(con.execute(f"SELECT run_id, count(exceso) FROM ({sql}) t GROUP BY 1").fetchall())

    assert solo_antiguas == (92, 0, 0)  # sin ninguna ejecución nueva la consulta funciona: columnas vacías
    assert por_ejecucion["run-antiguo"] == 0 and por_ejecucion["run-nuevo"] > 0
