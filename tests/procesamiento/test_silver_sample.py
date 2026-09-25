from datetime import date

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
)

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


def test_festivos_nacionales_computes_good_friday():
    assert festivos_nacionales(2024)[date(2024, 3, 29)] == "Viernes Santo"
    assert festivos_nacionales(2025)[date(2025, 4, 18)] == "Viernes Santo"
    assert festivos_nacionales(2026)[date(2026, 4, 3)] == "Viernes Santo"
    assert len(festivos_nacionales(2025)) == 10
