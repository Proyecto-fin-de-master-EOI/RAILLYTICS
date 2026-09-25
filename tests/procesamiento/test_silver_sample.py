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
    assert not viajeros.duplicated(["fecha", "estacion_id", "linea_id"]).any()
    assert (viajeros.viajeros > 0).all()
    assert set(viajeros.tipo_tren) == set(TIPOS_TREN)
    assert set(viajeros.condicion_meteo) <= set(CONDICIONES_METEO)


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
