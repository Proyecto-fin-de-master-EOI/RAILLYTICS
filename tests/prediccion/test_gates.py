import pandas as pd
import pytest

from raillytics.calidad.registro import (
    RESULTADO_FALLO,
    RESULTADO_OK,
    SEVERIDAD_AVISO,
    SEVERIDAD_BLOQUEANTE,
    QualityGateError,
    exigir,
)
from raillytics.prediccion.gates import DIAS_SEMANA, TABLA, dia_semana_del_motivo, evaluar_gates
from raillytics.prediccion.normalizar import normalizar_indices, repartir
from raillytics.prediccion.trimestre import Trimestre

DIAS = Trimestre(2026, 4).dias()
TOTAL = 1_320_000
BLOQUEANTES = [
    "un_registro_por_dia", "sin_nulos", "viajeros_no_negativos", "indice_en_rango", "suma_igual_total", "dia_semana_del_motivo",
]


def _crudos(indices=None):
    return indices or [1.0 + 0.1 * (i % 3) for i in range(len(DIAS))]


def _df(total=TOTAL, indices=None):
    indices = _crudos(indices)
    return pd.DataFrame(
        {
            "fecha": [d.isoformat() for d in DIAS],
            "viajeros_previstos": repartir(indices, total),
            "indice": normalizar_indices(indices),
            "motivo": [f"{DIAS_SEMANA[d.weekday()]} laborable" for d in DIAS],
        }
    )


def _evaluar(df, total=TOTAL, eventos=True, crudos=None):
    resultados = evaluar_gates(df, DIAS, total, eventos, indices_crudos=_crudos(crudos))
    return {r.gate: r for r in resultados}


def test_una_salida_correcta_pasa_todos_los_gates():
    resultados = evaluar_gates(_df(), DIAS, TOTAL, True, indices_crudos=_crudos())

    assert [r.gate for r in resultados] == BLOQUEANTES + ["indice_no_plano", "eventos_con_datos"]
    assert all(r.resultado == RESULTADO_OK and r.tabla == TABLA for r in resultados)
    assert [r.severidad for r in resultados] == [SEVERIDAD_BLOQUEANTE] * 6 + [SEVERIDAD_AVISO] * 2
    exigir(resultados)  # no lanza


def test_faltan_dias():
    resultado = _evaluar(_df().drop(index=[3, 4]))["un_registro_por_dia"]

    assert resultado.bloquea and resultado.valor == 90 and resultado.umbral == "92"
    assert "2026-10-04" in resultado.detalle and "2026-10-05" in resultado.detalle


def test_dias_duplicados_o_ajenos():
    df = _df()
    df.loc[1, "fecha"] = df.loc[0, "fecha"]  # 92 filas, pero una fecha repetida y otra ausente

    resultado = _evaluar(df)["un_registro_por_dia"]

    assert resultado.bloquea and "duplicadas" in resultado.detalle


def test_nulos():
    df = _df()
    df.loc[5, "indice"] = None

    assert _evaluar(df)["sin_nulos"].bloquea


def test_viajeros_negativos():
    df = _df()
    df.loc[0, "viajeros_previstos"] = -1

    assert _evaluar(df)["viajeros_no_negativos"].bloquea


def test_un_indice_crudo_fuera_de_rango_bloquea():
    crudos = [3.5] + [1.0] * 91

    resultado = _evaluar(_df(indices=crudos), crudos=crudos)["indice_en_rango"]

    assert resultado.bloquea and "fuera de rango" in resultado.detalle


def test_un_indice_legitimo_en_el_limite_no_bloquea_aunque_al_normalizar_baje_de_0_2():
    # Fines de semana a 1.3, laborables a 1.0 y dos festivos a 0.2: todo dentro del contrato del LLM.
    crudos = [0.2 if i in (10, 40) else (1.3 if DIAS[i].weekday() >= 5 else 1.0) for i in range(len(DIAS))]
    df = _df(indices=crudos)

    assert df["indice"].min() < 0.2  # media > 1.0: el 0.2 crudo queda por debajo de 0.2 al normalizar
    assert not _evaluar(df, crudos=crudos)["indice_en_rango"].bloquea


def test_suma_distinta_del_total():
    df = _df()
    df.loc[0, "viajeros_previstos"] += 1

    resultado = _evaluar(df)["suma_igual_total"]

    assert resultado.bloquea and resultado.valor == TOTAL + 1 and resultado.umbral == str(TOTAL)


@pytest.mark.parametrize(
    ("motivo", "esperado"),
    [
        ("miércoles, víspera del puente", 2),
        ("MIERCOLES festivo", 2),  # sin tilde y en mayúsculas
        ("Sabado festivo: conserva el valor del sábado", 5),
        ("domingo de regreso tras el lunes festivo", 6),  # cuenta el primero que nombra
        ("festivo nacional", None),
        ("lunesada", None),  # solo palabras completas
    ],
)
def test_dia_semana_del_motivo(motivo, esperado):
    assert dia_semana_del_motivo(motivo) == esperado


def test_una_respuesta_desplazada_un_dia_bloquea():
    # Como 2025-T3 con demanda_v3: fechas correctas, pero cada motivo (e índice) es el del día anterior.
    df = _df()
    df["motivo"] = [f"{DIAS_SEMANA[(d.weekday() - 1) % 7]} laborable" for d in DIAS]

    resultado = _evaluar(df)["dia_semana_del_motivo"]

    assert resultado.bloquea and resultado.valor == 92 and resultado.umbral == "<= 4"
    assert "2026-10-01 dice miércoles" in resultado.detalle  # el 1 de octubre de 2026 es jueves


def test_unos_pocos_dias_con_otro_dia_de_la_semana_no_bloquean_y_los_motivos_sin_dia_no_cuentan():
    df = _df()
    df.loc[:3, "motivo"] = "domingo de regreso"  # 1 a 4 de octubre: el 4 sí es domingo, así que 3 equivocados
    df.loc[10:30, "motivo"] = "festivo nacional"  # no nombra ningún día: no cuenta
    assert not _evaluar(df)["dia_semana_del_motivo"].bloquea

    df.loc[4:8, "motivo"] = "domingo de regreso"  # 5 a 9 de octubre: de lunes a viernes, ya pasan del 5 %
    resultado = _evaluar(df)["dia_semana_del_motivo"]
    assert resultado.bloquea and resultado.valor == 8


def test_con_fechas_llm_el_gate_solo_revisa_esos_motivos_y_con_pocos_dias_no_tolera_ninguno():
    df = _df()
    df.loc[57, "motivo"] = "sábado con concierto"  # 2026-11-27 es viernes

    assert not _evaluar(df)["dia_semana_del_motivo"].bloquea  # 1 de 92: dentro del 5 %
    resultados = evaluar_gates(df, DIAS, TOTAL, True, indices_crudos=_crudos(), fechas_llm=["2026-11-27", "2026-11-29"])
    resultado = {r.gate: r for r in resultados}["dia_semana_del_motivo"]
    assert resultado.bloquea and resultado.valor == 1 and resultado.umbral == "<= 0"


def test_un_indice_plano_solo_avisa():
    resultados = evaluar_gates(_df(indices=[1.0] * len(DIAS)), DIAS, TOTAL, True, indices_crudos=[1.0] * len(DIAS))
    por_nombre = {r.gate: r for r in resultados}

    assert por_nombre["indice_no_plano"].resultado == RESULTADO_FALLO
    assert not por_nombre["indice_no_plano"].bloquea
    exigir(resultados)  # un aviso no bloquea


def test_sin_datos_de_eventos_solo_avisa():
    resultados = evaluar_gates(_df(), DIAS, TOTAL, False, indices_crudos=_crudos())
    por_nombre = {r.gate: r for r in resultados}

    assert por_nombre["eventos_con_datos"].resultado == RESULTADO_FALLO and not por_nombre["eventos_con_datos"].bloquea
    exigir(resultados)


def test_un_gate_bloqueante_fallido_hace_que_exigir_lance():
    df = _df()
    df.loc[0, "viajeros_previstos"] += 1

    with pytest.raises(QualityGateError, match="suma_igual_total"):
        exigir(evaluar_gates(df, DIAS, TOTAL, True, indices_crudos=_crudos()))


def test_una_salida_vacia_falla_sin_romper_la_evaluacion():
    por_nombre = _evaluar(_df().iloc[0:0])

    assert por_nombre["un_registro_por_dia"].bloquea and por_nombre["suma_igual_total"].bloquea
