from datetime import date, timedelta

import duckdb
import pandas as pd
import pytest

from raillytics.procesamiento.silver_sample import (
    CONDICIONES_METEO,
    ESTADOS,
    OPERADORES as OPERADORES_DEF,
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


def test_market_shares_reproduce_the_cnmc_split(sample):
    # Las cuotas de OPERADORES ya no son inventadas: salen de la columna `Empresa` de CNMC (ver la
    # cabecera de silver_sample y `make cuotas-operador`). Así que lo que hay que comprobar es que la
    # tabla generada reproduce el reparto MEDIDO.
    #
    # El contraste va por EMPRESA, que es el grano en que CNMC publica: Avlo es la low cost de Renfe
    # Viajeros y CNMC no las separa, así que su reparto interno sigue siendo un supuesto y no es lo
    # que este test defiende.
    cuota = sample.viajeros.groupby("operador_id").viajeros.sum()
    cuota = (cuota / cuota.sum()).to_dict()

    assert cuota["RENFE"] + cuota["AVLO"] == pytest.approx(0.603, abs=0.02)  # Renfe Viajeros
    assert cuota["IRYO"] == pytest.approx(0.237, abs=0.02)
    assert cuota["OUIGO"] == pytest.approx(0.160, abs=0.02)
    # Solo se ordena lo que CNMC separa: Avlo y Ouigo quedan a dos décimas de punto una de otra, y
    # cuál sale delante depende de los factores de fin de semana, no del dato.
    assert cuota["RENFE"] > cuota["IRYO"] > cuota["OUIGO"]


def test_the_shares_add_up_to_one(sample):
    # Es el invariante que documenta OPERADORES: si no suman 1, la demanda total del corredor cambia
    # sin que nadie lo haya decidido.
    assert sum(op.cuota for op in OPERADORES_DEF) == pytest.approx(1.0)


def test_each_operator_runs_its_own_number_of_services_per_day(sample):
    por_dia = sample.puntualidad.groupby(["fecha", "operador_id"]).size().unstack()

    assert (por_dia["RENFE"] == 12).all() and (por_dia["IRYO"] == 7).all()
    assert (por_dia["OUIGO"] == 5).all() and (por_dia["AVLO"] == 4).all()
    assert (por_dia.sum(axis=1) == 28).all()
    assert sample.puntualidad.servicio_id.is_unique and sample.puntualidad.servicio_id.str.contains("AVE-MAD-BCN-").all()


def test_operators_differ_in_punctuality(sample):
    retraso = sample.puntualidad.groupby("operador_id").retraso_minutos.mean()

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
    assert set(puntualidad.estado_servicio) == set(ESTADOS)
    assert (puntualidad.hora_prevista.dt.date == puntualidad.fecha.dt.date).all()

    cancelados = puntualidad[puntualidad.estado_servicio == "cancelado"]
    realizados = puntualidad[puntualidad.estado_servicio == "realizado"]
    assert len(cancelados) > 0
    assert cancelados.retraso_minutos.isna().all() and cancelados.hora_real.isna().all()
    assert (realizados.retraso_minutos >= 0).all()
    esperado = realizados.hora_prevista + pd.to_timedelta(realizados.retraso_minutos.astype("int64"), unit="m")
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


# --- enriquecimiento real: festivos del BOE y meteo de AEMET ---

def test_sin_enriquecimiento_real_el_generador_sigue_funcionando_solo():
    # Es lo que permite probarlo sin lago y sin red, y generar la muestra en un clon recién hecho.
    s = generate_silver_sample(START, END, seed=7)

    assert not s.viajeros.empty
    assert s.viajeros["meteo_imputada"].all(), "sin AEMET, toda la meteo está simulada"


def test_la_meteo_de_aemet_sustituye_a_la_simulada_y_el_resto_queda_marcado():
    # AEMET solo cubre las dos cabeceras: a Zaragoza y Tarragona NO se les asigna la meteo de otra
    # ciudad, porque su clima es distinto. Se quedan simuladas y marcadas.
    fechas = pd.date_range(START, END, freq="D")
    real = pd.DataFrame({
        "fecha": list(fechas) * 2,
        "ciudad": ["MAD"] * len(fechas) + ["BCN"] * len(fechas),
        "temperatura_media": [11.5] * len(fechas) + [19.5] * len(fechas),
        "precipitacion_mm": [0.0] * len(fechas) * 2,
    })

    s = generate_silver_sample(START, END, seed=7, meteo_real=real)
    v = s.viajeros

    assert (v.loc[v.provincia == "Madrid", "temperatura_media"] == 11.5).all()
    assert (v.loc[v.provincia == "Barcelona", "temperatura_media"] == 19.5).all()
    assert not v.loc[v.provincia == "Madrid", "meteo_imputada"].any()
    assert v.loc[v.provincia == "Zaragoza", "meteo_imputada"].all()


def test_un_dia_que_aemet_no_publico_se_queda_simulado_y_marcado():
    # AEMET deja huecos. La diferencia entre un hueco declarado y un dato inventado que parece
    # medido es justo esta marca.
    fechas = pd.date_range(START, END, freq="D")
    real = pd.DataFrame({
        "fecha": [fechas[0]], "ciudad": ["MAD"],
        "temperatura_media": [11.5], "precipitacion_mm": [0.0],
    })

    v = generate_silver_sample(START, END, seed=7, meteo_real=real).viajeros
    madrid = v[v.provincia == "Madrid"]

    assert not madrid[madrid.fecha == fechas[0]]["meteo_imputada"].any()
    assert madrid[madrid.fecha != fechas[0]]["meteo_imputada"].all()


def test_los_festivos_reales_sustituyen_a_los_nacionales_por_regla():
    # El BOE trae también los de Madrid y Cataluña, y para un corredor que une las dos ciudades eso
    # cambia qué días son festivos.
    propios = {START + timedelta(days=3): "Fiesta inventada (Cataluña)"}

    v = generate_silver_sample(START, END, seed=7, festivos_reales=propios).viajeros

    assert set(v.loc[v.es_festivo, "festivo_nombre"]) == {"Fiesta inventada (Cataluña)"}
