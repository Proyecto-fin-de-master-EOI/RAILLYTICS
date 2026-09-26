import logging
from pathlib import Path

import duckdb
import pytest

from raillytics.calidad.registro import (
    CALIDAD_COLUMNS,
    QualityGateError,
    ResultadoGate,
    exigir,
    leer_calidad,
    registrar_calidad,
)
from raillytics.utils.lake import LakeLayout, connect


@pytest.fixture
def layout(tmp_path):
    return LakeLayout(silver_root=(tmp_path / "silver").as_posix(), gold_root=(tmp_path / "gold").as_posix())


def _resultados():
    return [
        ResultadoGate("crtm", "contenido_no_vacio", "fichero", "bloqueante", "ok", valor=6042.0, umbral="> 0"),
        ResultadoGate("crtm", "formato_declarado", "fichero", "bloqueante", "fallo", valor=0.0, umbral="= 1",
                      detalle="declarado csv, el contenido parece ZIP"),
        ResultadoGate("crtm", "content_type", "fichero", "aviso", "fallo", valor=0.0, umbral="= 1", detalle="Content-Type raro"),
    ]


def test_registrar_calidad_escribe_una_fila_por_gate_con_el_esquema_comun(layout):
    con = connect()

    registrar_calidad(_resultados(), "bronze_download", "bronze", "20260925T100000-bronze_download-ab12cd", layout, con, lanzado_por="cli")

    filas = duckdb.connect().execute(
        f"SELECT {', '.join(n for n, _ in CALIDAD_COLUMNS)} FROM read_parquet('{layout.calidad_glob()}') ORDER BY gate"
    ).fetchall()
    assert [f[4] for f in filas] == ["contenido_no_vacio", "content_type", "formato_declarado"]
    (run_id, proceso, capa, tabla, gate, tipo, severidad, resultado, valor, umbral, detalle,
     inicio, fin, duracion, lanzado_por, ejecutor, usuario) = filas[2]
    assert (run_id, proceso, capa, tabla, gate, tipo) == (
        "20260925T100000-bronze_download-ab12cd", "bronze_download", "bronze", "crtm", "formato_declarado", "fichero")
    assert (severidad, resultado, valor, umbral, detalle) == ("bloqueante", "fallo", 0.0, "= 1", "declarado csv, el contenido parece ZIP")
    assert fin >= inicio and duracion == pytest.approx((fin - inicio).total_seconds())
    assert (lanzado_por, bool(ejecutor)) == ("cli", True)
    assert layout.calidad_file(run_id).endswith("/_trazabilidad/calidad/20260925T100000-bronze_download-ab12cd.parquet")


def test_leer_calidad_devuelve_lo_mas_reciente_primero(layout):
    con = connect()
    registrar_calidad(_resultados()[:1], "bronze_download", "bronze", "run-1", layout, con)
    registrar_calidad([ResultadoGate("gold_dim_fecha", "clave_unica", "unico", "bloqueante", "ok", 0.0, "= 0")],
                      "gold_build", "gold", "run-2", layout, con)

    filas = leer_calidad(con, layout, limit=5)

    assert [(f[1], f[3], f[4], f[6]) for f in filas] == [
        ("gold_build", "gold_dim_fecha", "clave_unica", "ok"),
        ("bronze_download", "crtm", "contenido_no_vacio", "ok"),
    ]


def test_exigir_lanza_solo_por_fallos_bloqueantes():
    resultados = _resultados()

    with pytest.raises(QualityGateError) as excinfo:
        exigir(resultados)
    assert [r.gate for r in excinfo.value.fallidos] == ["formato_declarado"]
    assert "1 quality gate(s) bloqueante(s) fallido(s): crtm.formato_declarado (valor=0, umbral='= 1'): declarado csv" in str(excinfo.value)

    exigir([r for r in resultados if r.gate != "formato_declarado"])   # solo queda un aviso: no lanza


def test_no_poder_registrar_no_rompe_la_carga(caplog):
    inaccesible = LakeLayout(silver_root="s3://no-existe", gold_root="s3://no-existe")

    with caplog.at_level(logging.ERROR, logger="raillytics.calidad.registro"):
        registrar_calidad(_resultados(), "bronze_download", "bronze", "run-x", inaccesible, connect())

    assert "no se pudieron registrar los resultados de quality gates" in caplog.text


def test_sin_resultados_no_escribe_nada(layout):
    registrar_calidad([], "bronze_download", "bronze", "run-vacio", layout, connect())

    assert not Path(layout.calidad_dir).exists()
