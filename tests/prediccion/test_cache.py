"""Caché de resultados del LLM: la misma entrada (prompt completo, modelo, semilla...) devuelve la misma respuesta al instante."""
import json
from dataclasses import replace
from datetime import date, timedelta

import pytest

from raillytics.prediccion.cache import CacheLLM, ClienteConCache
from raillytics.prediccion.normalizar import IndiceDia
from raillytics.prediccion.ollama import OllamaSettings, RespuestaInvalida

DIAS = [date(2026, 10, 1) + timedelta(days=i) for i in range(3)]
AJUSTES = OllamaSettings(url="http://x", modelo="m", num_ctx=1024, timeout_s=5, seed=7)


def indices(valor=1.1):
    return [IndiceDia(d, valor, f"día {d.day}") for d in DIAS]


class ClienteFalso:
    def __init__(self, error=None):
        self.settings = AJUSTES
        self.comprobaciones = 0
        self.llamadas = []
        self._error = error

    def comprobar(self):
        self.comprobaciones += 1

    def generar_indices(self, prompt, dias, reintentos=2):
        self.llamadas.append(prompt)
        if self._error:
            raise self._error
        return indices()


def test_la_clave_depende_de_todo_lo_que_cambia_la_respuesta(tmp_path):
    cache = CacheLLM(tmp_path)
    base = cache.clave("PROMPT", AJUSTES, 3)

    assert cache.clave("PROMPT", AJUSTES, 3) == base and len(base) == 64
    distintas = [
        cache.clave("OTRO PROMPT", AJUSTES, 3),
        cache.clave("PROMPT", replace(AJUSTES, modelo="otro"), 3),
        cache.clave("PROMPT", replace(AJUSTES, seed=8), 3),
        cache.clave("PROMPT", replace(AJUSTES, num_ctx=2048), 3),
        cache.clave("PROMPT", AJUSTES, 4),
    ]
    assert base not in distintas and len(set(distintas)) == len(distintas)


def test_la_clave_no_depende_de_lo_que_no_cambia_la_respuesta(tmp_path):
    cache = CacheLLM(tmp_path)

    assert cache.clave("PROMPT", replace(AJUSTES, url="http://otro:1", timeout_s=1), 3) == cache.clave("PROMPT", AJUSTES, 3)


def test_guardar_y_leer_devuelven_lo_mismo(tmp_path):
    cache = CacheLLM(tmp_path)
    clave = cache.clave("PROMPT", AJUSTES, 3)

    cache.guardar(clave, indices(), AJUSTES)

    assert cache.leer(clave, DIAS) == indices()
    assert json.loads((tmp_path / f"{clave}.json").read_text(encoding="utf-8"))["modelo"] == "m"


def test_sin_entrada_no_hay_acierto(tmp_path):
    assert CacheLLM(tmp_path).leer("a" * 64, DIAS) is None


def test_una_entrada_corrupta_o_de_otros_dias_se_ignora(tmp_path):
    cache = CacheLLM(tmp_path)
    clave = cache.clave("PROMPT", AJUSTES, 3)
    (tmp_path / f"{clave}.json").write_text("{esto no es json", encoding="utf-8")
    assert cache.leer(clave, DIAS) is None

    cache.guardar(clave, indices(), AJUSTES)
    otros_dias = [d + timedelta(days=30) for d in DIAS]
    assert cache.leer(clave, otros_dias) is None  # el contenido no corresponde a los días pedidos

    (tmp_path / f"{clave}.json").write_text(json.dumps({"dias": [{"fecha": "2026-10-01", "indice": 99, "motivo": "x"}]}), encoding="utf-8")
    assert cache.leer(clave, DIAS) is None  # y un contenido que ya no cumple el contrato tampoco se acepta


def test_la_cache_es_de_mejor_esfuerzo_si_no_se_puede_escribir(tmp_path):
    fichero = tmp_path / "no_es_un_directorio"
    fichero.write_text("x", encoding="utf-8")
    cache = CacheLLM(fichero)

    cache.guardar("a" * 64, indices(), AJUSTES)  # no lanza

    assert cache.leer("a" * 64, DIAS) is None


def test_la_segunda_peticion_identica_no_llama_al_llm_ni_comprueba_ollama(tmp_path):
    real = ClienteFalso()
    mensajes = []
    cliente = ClienteConCache(real, CacheLLM(tmp_path), mensajes.append)

    primera = cliente.generar_indices("PROMPT", DIAS)
    segunda = cliente.generar_indices("PROMPT", DIAS)

    assert primera == segunda == indices()
    assert real.llamadas == ["PROMPT"] and real.comprobaciones == 1  # solo la primera vez
    assert cliente.resultado_en_cache is True
    assert any("caché" in m and "--sin-cache" in m for m in mensajes)


def test_un_prompt_distinto_vuelve_a_llamar_al_llm(tmp_path):
    real = ClienteFalso()
    cliente = ClienteConCache(real, CacheLLM(tmp_path), lambda _: None)

    cliente.generar_indices("PROMPT", DIAS)
    cliente.generar_indices("PROMPT CAMBIADO", DIAS)

    assert real.llamadas == ["PROMPT", "PROMPT CAMBIADO"] and cliente.resultado_en_cache is False


def test_los_errores_del_llm_no_se_cachean(tmp_path):
    real = ClienteFalso(error=RespuestaInvalida("faltan días"))
    cliente = ClienteConCache(real, CacheLLM(tmp_path), lambda _: None)

    for _ in range(2):
        with pytest.raises(RespuestaInvalida):
            cliente.generar_indices("PROMPT", DIAS)

    assert len(real.llamadas) == 2 and not list(tmp_path.glob("*.json"))


def test_expone_los_ajustes_del_cliente_y_su_comprobar_es_diferido(tmp_path):
    real = ClienteFalso()
    cliente = ClienteConCache(real, CacheLLM(tmp_path), lambda _: None)

    cliente.comprobar()  # no-op: solo hace falta Ollama si hay que generar

    assert cliente.settings is AJUSTES and real.comprobaciones == 0 and cliente.resultado_en_cache is None
