import json
import logging

import duckdb
import pytest

from raillytics.utils.cargas import CARGAS_COLUMNS, lanzado_por_defecto, leer_cargas, registrar_carga
from raillytics.utils.lake import LakeLayout, connect


@pytest.fixture
def layout(tmp_path):
    return LakeLayout(silver_root=(tmp_path / "silver").as_posix(), gold_root=(tmp_path / "gold").as_posix())


def cargas(layout):
    columnas = ", ".join(nombre for nombre, _ in CARGAS_COLUMNS)
    return duckdb.connect().execute(
        f"SELECT {columnas} FROM read_parquet('{layout.cargas_glob()}') ORDER BY inicio, tabla"
    ).fetchall()


def test_registra_una_fila_por_tabla_con_parametros_y_duracion(layout):
    con = connect()

    with registrar_carga("gold_build", "gold", layout, con, parametros={"umbral": 5}, lanzado_por="cli") as ejecucion:
        with ejecucion.tabla("dim_fecha", origen="silver", destino="gold/dim_fecha.parquet") as carga:
            carga.filas = 365
        with ejecucion.tabla("fact_viajeros", origen="silver", destino="gold/fact_viajeros.parquet") as carga:
            carga.filas, carga.bytes = 15330, 1024

    filas = cargas(layout)
    assert [fila[3] for fila in filas] == ["dim_fecha", "fact_viajeros"]
    (run_id, proceso, capa, tabla, origen, destino, n, nbytes, inicio, fin, duracion,
     estado, error, params, lanzado, ejecutor, usuario) = filas[0]
    assert run_id == ejecucion.run_id and run_id.split("-")[1] == "gold_build"
    assert (proceso, capa, origen, destino, n, nbytes) == ("gold_build", "gold", "silver", "gold/dim_fecha.parquet", 365, None)
    assert (estado, error, lanzado) == ("ok", None, "cli")
    assert json.loads(params) == {"umbral": 5}
    assert fin >= inicio and duracion == pytest.approx((fin - inicio).total_seconds())
    assert filas[1][7] == 1024
    assert {fila[0] for fila in filas} == {ejecucion.run_id}
    assert ejecutor and usuario


def test_error_dentro_de_una_tabla_se_registra_y_se_propaga(layout):
    con = connect()

    with pytest.raises(ValueError, match="parquet corrupto"):
        with registrar_carga("gold_build", "gold", layout, con) as ejecucion:
            with ejecucion.tabla("dim_fecha") as carga:
                carga.filas = 10
            with ejecucion.tabla("dim_linea"):
                raise ValueError("parquet corrupto")

    filas = cargas(layout)
    assert [(fila[3], fila[11], fila[6]) for fila in filas] == [("dim_fecha", "ok", 10), ("dim_linea", "error", None)]
    assert filas[1][12] == "ValueError: parquet corrupto"


def test_error_fuera_de_las_tablas_deja_una_fila_de_ejecucion(layout):
    con = connect()

    with pytest.raises(RuntimeError):
        with registrar_carga("silver_sample", "silver", layout, con):
            raise RuntimeError("MinIO no responde")

    filas = cargas(layout)
    assert len(filas) == 1
    assert (filas[0][3], filas[0][11], filas[0][12]) == (None, "error", "RuntimeError: MinIO no responde")


def test_una_ejecucion_sin_tablas_tambien_queda_registrada(layout):
    with registrar_carga("bronze_download", "bronze", layout, connect()):
        pass

    filas = cargas(layout)
    assert [(fila[1], fila[3], fila[11]) for fila in filas] == [("bronze_download", None, "ok")]


def test_no_poder_escribir_el_registro_no_rompe_la_carga(caplog):
    # s3:// sin credenciales configuradas: la escritura del registro falla, la carga no.
    inaccesible = LakeLayout(silver_root="s3://no-existe", gold_root="s3://no-existe")

    with caplog.at_level(logging.ERROR, logger="raillytics.utils.cargas"):
        with registrar_carga("gold_build", "gold", inaccesible, connect()) as ejecucion:
            with ejecucion.tabla("dim_fecha") as carga:
                carga.filas = 1

    assert "no se pudo registrar la trazabilidad" in caplog.text


def test_leer_cargas_devuelve_las_mas_recientes_primero(layout):
    con = connect()
    with registrar_carga("silver_sample", "silver", layout, con) as ejecucion:
        with ejecucion.tabla("viajeros_enriquecidos") as carga:
            carga.filas = 3
    with registrar_carga("gold_build", "gold", layout, con) as ejecucion:
        with ejecucion.tabla("dim_fecha") as carga:
            carga.filas = 7

    ultimas = leer_cargas(con, layout, limit=5)
    assert [(fila[1], fila[3], fila[4]) for fila in ultimas] == [
        ("gold_build", "dim_fecha", 7),
        ("silver_sample", "viajeros_enriquecidos", 3),
    ]


def test_lanzado_por_se_deduce_del_entorno():
    assert lanzado_por_defecto({}) == "cli"
    assert lanzado_por_defecto({"MAKELEVEL": "1"}) == "make"
    assert lanzado_por_defecto({"AIRFLOW_CTX_DAG_ID": "ingesta_data_sources", "MAKELEVEL": "1"}) == "airflow:ingesta_data_sources"
