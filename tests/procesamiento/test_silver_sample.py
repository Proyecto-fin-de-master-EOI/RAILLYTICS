from datetime import date

import duckdb
import pandas as pd
import pytest

from raillytics.procesamiento.silver_sample import (
    CONDICIONES_METEO,
    ESTADOS,
    PUNTUALIDAD_COLUMNS,
    TIPOS_TREN,
    VIAJEROS_COLUMNS,
    festivos_nacionales,
    generate_silver_sample,
    write_silver_sample,
)
from raillytics.utils.lake import LakeLayout, connect

START, END = date(2025, 4, 14), date(2025, 5, 4)


@pytest.fixture(scope="module")
def sample():
    return generate_silver_sample(START, END, seed=7)


def test_generation_is_deterministic_for_the_same_seed(sample):
    again = generate_silver_sample(START, END, seed=7)
    other = generate_silver_sample(START, END, seed=8)

    pd.testing.assert_frame_equal(sample.viajeros, again.viajeros)
    pd.testing.assert_frame_equal(sample.puntualidad, again.puntualidad)
    assert not sample.viajeros.viajeros.equals(other.viajeros.viajeros)


def test_viajeros_follow_the_silver_contract(sample):
    viajeros = sample.viajeros

    assert tuple(viajeros.columns) == VIAJEROS_COLUMNS
    assert viajeros.fecha.min().date() == START and viajeros.fecha.max().date() == END
    assert not viajeros.drop(columns="festivo_nombre").isna().any().any()
    assert not viajeros.duplicated(["fecha", "estacion_id", "linea_id", "operador_id"]).any()
    assert (viajeros.viajeros > 0).all()
    assert set(viajeros.tipo_tren) == set(TIPOS_TREN)
    assert set(viajeros.condicion_meteo) <= set(CONDICIONES_METEO)


def test_the_sample_covers_only_the_ave_madrid_barcelona_corridor(sample):
    estaciones = {"MADPA", "ZARDE", "TARRA", "BCNSA"}  # Atocha, Zaragoza Delicias, Camp de Tarragona, Sants

    assert set(sample.viajeros.linea_id) == set(sample.puntualidad.linea_id) == {"AVE-MAD-BCN"}
    assert set(sample.viajeros.estacion_id) == estaciones
    assert set(sample.puntualidad.estacion_id) <= estaciones
    assert set(sample.viajeros.tipo_tren) == {"AVE"}
    assert set(sample.viajeros.provincia) == {"Madrid", "Zaragoza", "Tarragona", "Barcelona"}
    assert TIPOS_TREN == ("AVE",)


OPERADORES = {"RENFE": "Renfe", "IRYO": "Iryo", "OUIGO": "Ouigo", "AVLO": "Avlo"}


def test_the_corridor_has_the_four_operators_with_consistent_attributes(sample):
    viajeros, puntualidad = sample.viajeros, sample.puntualidad

    assert set(viajeros.operador_id) == set(puntualidad.operador_id) == set(OPERADORES)
    assert viajeros.groupby("operador_id").operador_nombre.first().to_dict() == OPERADORES
    for columna in ("operador_nombre", "operador_empresa", "operador_segmento"):
        assert (viajeros.groupby("operador_id")[columna].nunique() == 1).all()  # cada operador, un solo valor
    assert set(viajeros.operador_segmento) == {"Alta velocidad", "Low cost"}
    assert viajeros.groupby("operador_id").operador_empresa.first()["AVLO"] == "Renfe Viajeros"  # Avlo es de Renfe


def test_market_shares_follow_the_model(sample):
    cuota = sample.viajeros.groupby("operador_id").viajeros.sum()
    cuota = (cuota / cuota.sum()).to_dict()

    assert cuota["RENFE"] == pytest.approx(0.40, abs=0.03) and cuota["AVLO"] == pytest.approx(0.15, abs=0.03)
    assert cuota["RENFE"] > cuota["IRYO"] > cuota["OUIGO"] > cuota["AVLO"]


def test_each_operator_runs_its_own_number_of_services_per_day(sample):
    por_dia = sample.puntualidad.groupby(["fecha", "operador_id"]).size().unstack()

    assert (por_dia["RENFE"] == 12).all() and (por_dia["IRYO"] == 7).all()
    assert (por_dia["OUIGO"] == 5).all() and (por_dia["AVLO"] == 4).all()
    assert (por_dia.sum(axis=1) == 28).all()
    assert sample.puntualidad.servicio_id.is_unique and sample.puntualidad.servicio_id.str.contains("AVE-MAD-BCN-").all()


def test_operators_differ_in_punctuality(sample):
    retraso = sample.puntualidad.groupby("operador_id").retraso_min.mean()

    assert retraso["OUIGO"] > retraso["IRYO"]  # el más lento y el más puntual del modelo


def test_holidays_are_flagged_with_their_name(sample):
    por_fecha = sample.viajeros.groupby(sample.viajeros.fecha.dt.date)

    assert set(por_fecha.es_festivo.get_group(date(2025, 4, 18))) == {True}
    assert set(por_fecha.festivo_nombre.get_group(date(2025, 4, 18))) == {"Viernes Santo"}
    assert set(por_fecha.festivo_nombre.get_group(date(2025, 5, 1))) == {"Fiesta del Trabajo"}
    assert set(por_fecha.es_festivo.get_group(date(2025, 4, 17))) == {False}
    assert set(por_fecha.festivo_nombre.get_group(date(2025, 4, 17))) == {None}


def test_puntualidad_follows_the_silver_contract(sample):
    puntualidad = sample.puntualidad

    assert tuple(puntualidad.columns) == PUNTUALIDAD_COLUMNS
    assert puntualidad.servicio_id.is_unique
    assert set(puntualidad.estado) == set(ESTADOS)
    assert (puntualidad.hora_prevista.dt.date == puntualidad.fecha.dt.date).all()

    cancelados = puntualidad[puntualidad.estado == "cancelado"]
    realizados = puntualidad[puntualidad.estado == "realizado"]
    assert len(cancelados) > 0
    assert cancelados.retraso_min.isna().all() and cancelados.hora_real.isna().all()
    assert (realizados.retraso_min >= 0).all()
    esperado = realizados.hora_prevista + pd.to_timedelta(realizados.retraso_min.astype("int64"), unit="m")
    assert (realizados.hora_real == esperado).all()


def test_write_leaves_parquet_and_a_trace_per_table(tmp_path, sample):
    layout = LakeLayout(silver_root=(tmp_path / "silver").as_posix(), gold_root=(tmp_path / "gold").as_posix())

    written = write_silver_sample(connect(), layout, sample)

    assert set(written) == {"viajeros_enriquecidos", "puntualidad_enriquecida"}
    con = duckdb.connect()
    assert con.execute(f"SELECT count(*) FROM read_parquet('{written['viajeros_enriquecidos']}')").fetchone() == (len(sample.viajeros),)
    assert con.execute(f"SELECT typeof(fecha), typeof(hora_prevista) FROM read_parquet('{written['puntualidad_enriquecida']}') LIMIT 1").fetchone() == ("DATE", "TIMESTAMP")
    trazas = con.execute(
        f"SELECT proceso, tabla, filas, estado, parametros FROM read_parquet('{layout.cargas_glob()}') ORDER BY tabla"
    ).fetchall()
    assert [(t[0], t[1], t[2], t[3]) for t in trazas] == [
        ("silver_sample", "puntualidad_enriquecida", len(sample.puntualidad), "ok"),
        ("silver_sample", "viajeros_enriquecidos", len(sample.viajeros), "ok"),
    ]
    assert '"seed": 7' in trazas[0][4]


def test_festivos_nacionales_computes_good_friday():
    assert festivos_nacionales(2024)[date(2024, 3, 29)] == "Viernes Santo"
    assert festivos_nacionales(2025)[date(2025, 4, 18)] == "Viernes Santo"
    assert festivos_nacionales(2026)[date(2026, 4, 3)] == "Viernes Santo"
    assert len(festivos_nacionales(2025)) == 10
