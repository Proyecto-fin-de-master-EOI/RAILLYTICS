import json
from datetime import datetime
from pathlib import Path

import duckdb
import pytest
import yaml

from raillytics.calidad.registro import QualityGateError
from raillytics.prediccion.entradas import EntradaError
from raillytics.prediccion.gates import TABLA
from raillytics.prediccion.nivel import NivelError
from raillytics.prediccion.normalizar import IndiceDia
from raillytics.prediccion.ollama import OllamaSettings, RespuestaInvalida
from raillytics.prediccion.prompt import PlantillaError
from raillytics.prediccion.servicio import ejecutar
from raillytics.prediccion.trimestre import Trimestre

T4 = Trimestre(2026, 4)
AHORA = datetime(2026, 10, 1, 16, 51, 0)


class ClienteFalso:
    """Sustituye a Ollama: indices deterministas (fines de semana más altos) o un error forzado."""

    def __init__(self, indices=None, error=None):
        self.settings = OllamaSettings(url="http://falso", modelo="falso", num_ctx=1024, timeout_s=1, seed=1)
        self.prompts = []
        self.comprobaciones = 0
        self._indices = indices
        self._error = error

    def comprobar(self):
        self.comprobaciones += 1

    def generar_indices(self, prompt, dias, reintentos=2):
        self.prompts.append(prompt)
        if self._error:
            raise self._error
        if self._indices is not None:
            return [IndiceDia(d, v, "forzado") for d, v in zip(dias, self._indices)]
        return [IndiceDia(d, 1.3 if d.weekday() >= 5 else 1.0, "finde" if d.weekday() >= 5 else "laborable") for d in dias]


def _lanzar(entorno, cliente, **extra):
    salida = []
    argumentos = dict(
        version_prompt="demanda_v1", total_manual=None, solo_nivel=False, env=entorno.env, layout=entorno.layout,
        con=entorno.con, cliente=cliente, ahora=AHORA, imprimir=salida.append,
    )
    argumentos.update(extra)
    return ejecutar(T4, **argumentos), salida


def _cargas(entorno):
    return duckdb.connect().execute(
        f"SELECT proceso, capa, tabla, destino, filas, estado, error, parametros "
        f"FROM read_parquet('{entorno.layout.cargas_glob()}')"
    ).fetchall()


def _calidad(entorno):
    return duckdb.connect().execute(
        f"SELECT gate, severidad, resultado FROM read_parquet('{entorno.layout.calidad_glob()}')"
    ).fetchall()


def _csvs(entorno):
    return sorted(entorno.salida.rglob("*.csv")) if entorno.salida.exists() else []


def test_ejecuta_de_principio_a_fin_y_escribe_el_csv(entorno):
    cliente = ClienteFalso()

    resultado, salida = _lanzar(entorno, cliente)

    assert resultado.total_esperado == 1_320_000 and resultado.nivel.total == 1_320_000
    assert resultado.ruta == entorno.salida / "AVE-MAD-BCN" / "2026-T4" / "demanda_diaria_2026-T4_demanda_v1_20261001T165100Z.csv"
    assert _csvs(entorno) == [resultado.ruta]
    assert len(resultado.dataframe) == 92 and resultado.dataframe["viajeros_previstos"].sum() == 1_320_000
    assert cliente.comprobaciones == 1 and "1.320.000" in cliente.prompts[0]
    texto = "\n".join(salida)
    assert "viajeros esperados" in texto and "CSV escrito" in texto and "Coherencia del reparto" in texto


def test_deja_constancia_de_la_carga_y_de_los_gates(entorno):
    resultado, _ = _lanzar(entorno, ClienteFalso())

    (proceso, capa, tabla, destino, filas, estado, error, parametros), = _cargas(entorno)
    assert (proceso, capa, tabla, estado, error, filas) == ("prediccion_demanda", "ml", TABLA, "ok", None, 92)
    assert destino == str(resultado.ruta)
    assert json.loads(parametros) == {
        "trimestre": "2026-T4", "modelo": "falso", "version_prompt": "demanda_v1", "seed": 1, "num_ctx": 1024,
        "total_manual": None,
    }
    assert len(_calidad(entorno)) == 7 and all(r == "ok" for _, _, r in _calidad(entorno))


def test_solo_nivel_no_llama_al_llm_ni_escribe_ni_registra(entorno):
    resultado, salida = _lanzar(entorno, None, solo_nivel=True)

    assert resultado.ruta is None and resultado.total_esperado == 1_320_000
    assert any("1.320.000" in linea for linea in salida)
    assert _csvs(entorno) == []
    assert not Path(entorno.layout.cargas_dir).exists()  # no hay carga registrada


def test_un_trimestre_ya_publicado_informa_de_la_desviacion_del_nivel(entorno):
    salida = []

    ejecutar(
        Trimestre(2026, 2), version_prompt="demanda_v1", total_manual=None, solo_nivel=True, env=entorno.env,
        layout=entorno.layout, con=entorno.con, cliente=None, imprimir=salida.append,
    )

    # esperado 1.100.000 × (1.050.000 / 1.000.000) = 1.155.000 frente a 1.210.000 reales
    assert any("ya está publicado" in linea and "-4.55%" in linea for linea in salida)


def test_total_manual_sustituye_al_nivel(entorno):
    resultado, _ = _lanzar(entorno, ClienteFalso(), total_manual=500_000)

    assert resultado.nivel is None and resultado.dataframe["viajeros_previstos"].sum() == 500_000
    assert json.loads(_cargas(entorno)[0][7])["total_manual"] == 500_000


def test_mostrar_prompt_imprime_exactamente_lo_que_se_envia(entorno):
    cliente = ClienteFalso()

    _, salida = _lanzar(entorno, cliente, mostrar_prompt=True)

    assert cliente.prompts[0] in salida


def test_si_el_llm_falla_no_se_escribe_nada_y_la_carga_queda_en_error(entorno):
    with pytest.raises(RespuestaInvalida):
        _lanzar(entorno, ClienteFalso(error=RespuestaInvalida("faltan días")))

    assert _csvs(entorno) == []
    (_, _, _, _, _, estado, error, _), = _cargas(entorno)
    assert estado == "error" and "faltan días" in error


def test_un_gate_bloqueante_impide_escribir_y_queda_registrado(entorno):
    extremos = [3.0] + [0.2] * 91  # normalizado, el máximo supera con mucho 3.0

    with pytest.raises(QualityGateError, match="indice_en_rango"):
        _lanzar(entorno, ClienteFalso(indices=extremos))

    assert _csvs(entorno) == []
    assert ("indice_en_rango", "bloqueante", "fallo") in _calidad(entorno)
    assert _cargas(entorno)[0][5] == "error"


def test_un_origen_ausente_falla_cerrado_y_queda_registrado(entorno):
    consultas = yaml.safe_load(entorno.config.read_text(encoding="utf-8"))
    consultas["origenes"]["eventos"]["sql"] = "SELECT * FROM read_parquet('{silver}/sin_ingestar/*.parquet')"
    entorno.config.write_text(yaml.safe_dump(consultas), encoding="utf-8")

    with pytest.raises(EntradaError, match="origen 'eventos'"):
        _lanzar(entorno, ClienteFalso())

    assert _csvs(entorno) == []
    assert _cargas(entorno)[0][5] == "error" and "origen 'eventos'" in _cargas(entorno)[0][6]


def test_una_plantilla_inexistente_falla_cerrado(entorno):
    with pytest.raises(PlantillaError, match="no existe"):
        _lanzar(entorno, ClienteFalso(), version_prompt="demanda_v99")

    assert _csvs(entorno) == [] and _cargas(entorno)[0][5] == "error"


def test_sin_nivel_calculable_pide_el_total_a_mano(entorno):
    with pytest.raises(NivelError, match="--total-esperado"):
        ejecutar(
            Trimestre(2028, 4), version_prompt="demanda_v1", total_manual=None, solo_nivel=False, env=entorno.env,
            layout=entorno.layout, con=entorno.con, cliente=ClienteFalso(), imprimir=lambda _: None,
        )

    assert _csvs(entorno) == []
