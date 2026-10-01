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
from raillytics.prediccion.gates import TABLA, evaluar_gates
from raillytics.prediccion.normalizar import normalizar_indices, repartir
from raillytics.prediccion.trimestre import Trimestre

DIAS = Trimestre(2026, 4).dias()
TOTAL = 1_320_000
BLOQUEANTES = ["un_registro_por_dia", "sin_nulos", "viajeros_no_negativos", "indice_en_rango", "suma_igual_total"]


def _df(total=TOTAL, indices=None):
    indices = indices or [1.0 + 0.1 * (i % 3) for i in range(len(DIAS))]
    return pd.DataFrame(
        {
            "fecha": [d.isoformat() for d in DIAS],
            "viajeros_previstos": repartir(indices, total),
            "indice": normalizar_indices(indices),
        }
    )


def _evaluar(df, total=TOTAL, eventos=True):
    return {r.gate: r for r in evaluar_gates(df, DIAS, total, eventos)}


def test_una_salida_correcta_pasa_todos_los_gates():
    resultados = evaluar_gates(_df(), DIAS, TOTAL, True)

    assert [r.gate for r in resultados] == BLOQUEANTES + ["indice_no_plano", "eventos_con_datos"]
    assert all(r.resultado == RESULTADO_OK and r.tabla == TABLA for r in resultados)
    assert [r.severidad for r in resultados] == [SEVERIDAD_BLOQUEANTE] * 5 + [SEVERIDAD_AVISO] * 2
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


def test_indice_fuera_de_rango_tras_normalizar():
    # Un día disparado y el resto bajo: normalizado a media 1.0 el máximo supera con mucho 3.0.
    resultado = _evaluar(_df(indices=[3.0] + [0.2] * 91))["indice_en_rango"]

    assert resultado.bloquea and "fuera de rango" in resultado.detalle


def test_suma_distinta_del_total():
    df = _df()
    df.loc[0, "viajeros_previstos"] += 1

    resultado = _evaluar(df)["suma_igual_total"]

    assert resultado.bloquea and resultado.valor == TOTAL + 1 and resultado.umbral == str(TOTAL)


def test_un_indice_plano_solo_avisa():
    resultados = evaluar_gates(_df(indices=[1.0] * len(DIAS)), DIAS, TOTAL, True)
    por_nombre = {r.gate: r for r in resultados}

    assert por_nombre["indice_no_plano"].resultado == RESULTADO_FALLO
    assert not por_nombre["indice_no_plano"].bloquea
    exigir(resultados)  # un aviso no bloquea


def test_sin_datos_de_eventos_solo_avisa():
    resultados = evaluar_gates(_df(), DIAS, TOTAL, False)
    por_nombre = {r.gate: r for r in resultados}

    assert por_nombre["eventos_con_datos"].resultado == RESULTADO_FALLO and not por_nombre["eventos_con_datos"].bloquea
    exigir(resultados)


def test_un_gate_bloqueante_fallido_hace_que_exigir_lance():
    df = _df()
    df.loc[0, "viajeros_previstos"] += 1

    with pytest.raises(QualityGateError, match="suma_igual_total"):
        exigir(evaluar_gates(df, DIAS, TOTAL, True))


def test_una_salida_vacia_falla_sin_romper_la_evaluacion():
    por_nombre = _evaluar(_df().iloc[0:0])

    assert por_nombre["un_registro_por_dia"].bloquea and por_nombre["suma_igual_total"].bloquea
