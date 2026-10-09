"""El perfil mensual: qué cuotas salen del INE, qué coeficientes propone y cómo se aplican."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from raillytics.prediccion.normalizar import IndiceDia, normalizar_indices
from raillytics.prediccion.perfil_mensual import coeficientes, cuotas_por_mes
from raillytics.prediccion.reglas import MESES, ReglasError, cargar_reglas, perfil_mensual

REGLAS_DEL_REPO = cargar_reglas(Path("config/reglas_demanda.yml"))


def _serie(valores: dict[tuple[int, int], float]) -> pd.DataFrame:
    """La serie del INE tal como la deja `turismo_mensual`: una fila por (año, mes)."""
    filas = [
        {"fecha": date(anio, mes, 1), "turistas": v, "anio": anio, "mes": mes, "trimestre": (mes - 1) // 3 + 1}
        for (anio, mes), v in valores.items()
    ]
    return pd.DataFrame(filas)


def test_la_cuota_de_un_mes_es_su_parte_del_trimestre():
    # Un trimestre en que el tercer mes vale el doble que los otros dos: 25 %, 25 %, 50 %.
    serie = _serie({(2024, 1): 100.0, (2024, 2): 100.0, (2024, 3): 200.0})

    cuotas = cuotas_por_mes(serie).set_index("mes")

    assert cuotas.loc[1, "cuota"] == pytest.approx(0.25)
    assert cuotas.loc[3, "cuota"] == pytest.approx(0.50)


def test_un_trimestre_incompleto_no_cuenta():
    # Con enero y febrero pero sin marzo, enero saldría con el 50 % y el perfil quedaría al revés.
    serie = _serie({(2024, 1): 100.0, (2024, 2): 100.0, (2024, 7): 50.0, (2024, 8): 50.0, (2024, 9): 50.0})

    cuotas = cuotas_por_mes(serie)

    assert sorted(cuotas["mes"]) == [7, 8, 9]


def test_los_anios_de_la_pandemia_quedan_fuera_del_promedio():
    normal = {(a, m): v for a in (2023, 2024) for m, v in ((1, 100.0), (2, 100.0), (3, 100.0))}
    pandemia = {(2020, 1): 100.0, (2020, 2): 100.0, (2020, 3): 1.0}

    cuotas = cuotas_por_mes(_serie({**normal, **pandemia})).set_index("mes")

    # Si 2020 entrara, marzo bajaría muy por debajo del tercio.
    assert cuotas.loc[3, "cuota"] == pytest.approx(1 / 3)
    assert cuotas.loc[3, "anios"] == 2


def test_un_mes_cuyo_efecto_no_se_distingue_del_neutro_se_queda_en_uno_clavado():
    # Tres años con cuotas casi iguales al tercio: la diferencia es ruido.
    serie = _serie({
        **{(a, m): v for a, (v1, v2, v3) in {2023: (100.0, 101.0, 99.0), 2024: (101.0, 99.0, 100.0),
                                             2025: (99.0, 100.0, 101.0)}.items()
           for m, v in ((1, v1), (2, v2), (3, v3))},
    })

    coef = coeficientes(cuotas_por_mes(serie))

    assert coef == {"ene": 1.0, "feb": 1.0, "mar": 1.0}


def test_un_mes_con_un_efecto_consistente_si_propone_coeficiente():
    # Marzo se lleva el 50 % del trimestre todos los años: 0.50 / (1/3) = 1.5. Sin dispersión entre
    # años la evidencia es máxima, así que el coeficiente tiene que salir aunque el error típico
    # sea cero (una división por cero daría NaN y el NaN se leería como «no significativo»).
    serie = _serie({
        (a, m): v for a in (2023, 2024, 2025) for m, v in ((1, 50.0), (2, 50.0), (3, 100.0))
    })

    coef = coeficientes(cuotas_por_mes(serie))

    assert coef["mar"] == pytest.approx(1.5)
    assert coef["ene"] == pytest.approx(0.75)


def test_el_perfil_escala_el_valor_del_dia_y_lo_deja_escrito_en_el_motivo():
    # Enero vale 0.901 en el fichero del repo.
    dias = [IndiceDia(date(2026, 1, 13), 1.0, "martes laborable")]

    [resultado] = perfil_mensual(dias, REGLAS_DEL_REPO)

    assert resultado.indice == pytest.approx(0.901)
    assert resultado.motivo == "martes laborable, perfil de ene (0.901)"


def test_un_mes_neutro_no_toca_el_valor_ni_ensucia_el_motivo():
    # Mayo está en 1.000 porque su efecto no es distinguible: el motivo no debe mencionarlo.
    dias = [IndiceDia(date(2026, 5, 12), 1.0, "martes laborable")]

    [resultado] = perfil_mensual(dias, REGLAS_DEL_REPO)

    assert (resultado.indice, resultado.motivo) == (1.0, "martes laborable")


def test_el_perfil_solo_cambia_el_reparto_dentro_del_trimestre_no_su_total():
    # El reparto se normaliza a media 1.0 antes de aplicar el total de CNMC, así que lo que el perfil
    # hace es mover peso entre días, nunca cambiar cuántos viajeros tiene el trimestre.
    dias = [IndiceDia(date(2026, 1, 1) + pd.Timedelta(days=i), 1.0, "día") for i in range(90)]
    dias = [IndiceDia(d.fecha.date() if hasattr(d.fecha, "date") else d.fecha, d.indice, d.motivo) for d in dias]

    con_perfil = normalizar_indices([d.indice for d in perfil_mensual(dias, REGLAS_DEL_REPO)])

    assert sum(con_perfil) == pytest.approx(len(dias))


def test_el_fichero_del_repo_declara_los_doce_meses():
    assert sorted(REGLAS_DEL_REPO.base_mes) == sorted(MESES)
    assert all(0.5 < v < 1.5 for v in REGLAS_DEL_REPO.base_mes.values())


def test_sin_base_mes_el_reparto_trata_igual_a_todos_los_meses(tmp_path):
    sin_perfil = tmp_path / "reglas.yml"
    sin_perfil.write_text(
        Path("config/reglas_demanda.yml").read_text(encoding="utf-8").split("base_mes:")[0],
        encoding="utf-8",
    )

    reglas = cargar_reglas(sin_perfil)

    assert set(reglas.base_mes.values()) == {1.0}


@pytest.mark.parametrize(
    ("bloque", "mensaje"),
    [
        ("base_mes:\n  ene: 0.9\n  feb: 1.0\n", "faltan mar"),
        ("base_mes:\n" + "".join(f"  {m}: 1.0\n" for m in MESES) + "  tri: 1.0\n", "no reconoce tri"),
        ("base_mes: 0.9\n", "tiene que ser un mapa"),
        ("base_mes:\n" + "".join(f"  {m}: {'x' if m == 'ene' else 1.0}\n" for m in MESES), "no es un número"),
    ],
)
def test_un_base_mes_a_medias_o_mal_escrito_falla_diciendo_que_es_base_mes(tmp_path, bloque, mensaje):
    # Con cuatro meses escritos y ocho olvidados el reparto saldría sesgado y nada lo diría.
    roto = tmp_path / "reglas.yml"
    roto.write_text(
        Path("config/reglas_demanda.yml").read_text(encoding="utf-8").split("base_mes:")[0] + bloque,
        encoding="utf-8",
    )

    with pytest.raises(ReglasError, match=mensaje):
        cargar_reglas(roto)
