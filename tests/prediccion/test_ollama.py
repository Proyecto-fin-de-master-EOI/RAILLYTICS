import json
from datetime import date, timedelta

import pytest
import requests
from urllib3.exceptions import ReadTimeoutError

from raillytics.prediccion.ollama import (
    ContextoInsuficiente,
    ModeloNoDescargado,
    OllamaClient,
    OllamaError,
    OllamaNoDisponible,
    OllamaSettings,
    RespuestaInvalida,
    esquema_respuesta,
)

URL = "http://ollama.test"
CHAT = f"{URL}/api/chat"
TAGS = f"{URL}/api/tags"
DIAS = [date(2026, 10, 1) + timedelta(days=i) for i in range(3)]


@pytest.fixture
def cliente():
    return OllamaClient(OllamaSettings(url=URL, modelo="phi4", num_ctx=1024, timeout_s=5, seed=7))


def contenido_ok(dias=DIAS, indice=1.2):
    return json.dumps({"dias": [{"fecha": d.isoformat(), "indice": indice, "motivo": f"día {d.day}"} for d in dias]})


def respuesta(contenido=None, **extra):
    cuerpo = {
        "message": {"role": "assistant", "content": contenido if contenido is not None else contenido_ok()},
        "done": True,
        "done_reason": "stop",
        "prompt_eval_count": 100,
        "eval_count": 50,
    }
    cuerpo.update(extra)
    return {"json": cuerpo}


def entrada(fecha="2026-10-01", indice=1.2, motivo="x"):
    return {"fecha": fecha, "indice": indice, "motivo": motivo}


# --- configuración y schema ---------------------------------------------------------------------


def test_los_valores_por_defecto_son_los_medidos():
    s = OllamaSettings.from_env({})

    assert (s.url, s.modelo, s.num_ctx, s.timeout_s, s.seed) == ("http://localhost:11435", "mistral-nemo", 12288, 900.0, 42)


def test_from_env_lee_las_variables_y_la_url_manda_sobre_el_puerto():
    env = {
        "OLLAMA_PORT": "11500", "OLLAMA_MODEL": "phi4", "OLLAMA_NUM_CTX": "16384",
        "OLLAMA_TIMEOUT_S": "60", "OLLAMA_SEED": "7",
    }

    assert OllamaSettings.from_env(env).url == "http://localhost:11500"
    s = OllamaSettings.from_env(dict(env, OLLAMA_URL="http://ollama:11434/"))
    assert (s.url, s.modelo, s.num_ctx, s.timeout_s, s.seed) == ("http://ollama:11434", "phi4", 16384, 60.0, 7)


def test_el_schema_pide_el_motivo_antes_del_indice_para_que_el_modelo_razone_antes_de_decidir():
    items = esquema_respuesta(92)["properties"]["dias"]["items"]

    assert list(items["properties"]) == ["fecha", "motivo", "indice"]
    assert items["required"] == ["fecha", "motivo", "indice"]


def test_el_schema_fija_el_numero_de_dias_y_evita_las_clases_de_escape_que_ollama_rechaza():
    esquema = esquema_respuesta(92)

    dias = esquema["properties"]["dias"]
    assert dias["minItems"] == dias["maxItems"] == 92
    assert dias["items"]["properties"]["indice"]["minimum"] == 0.2
    assert dias["items"]["properties"]["indice"]["maximum"] == 3.0
    assert "\\d" not in json.dumps(esquema)  # Ollama 0.13.3 devuelve HTTP 500 con '\d' en un pattern


# --- comprobar ----------------------------------------------------------------------------------


def test_comprobar_acepta_el_modelo_con_o_sin_etiqueta(cliente, requests_mock):
    requests_mock.get(TAGS, json={"models": [{"name": "phi4:latest"}]})

    cliente.comprobar()


def test_comprobar_dice_como_descargar_un_modelo_ausente(cliente, requests_mock):
    requests_mock.get(TAGS, json={"models": [{"name": "mistral-nemo:latest"}]})

    with pytest.raises(ModeloNoDescargado, match="ollama pull phi4"):
        cliente.comprobar()


def test_comprobar_dice_como_levantar_ollama(cliente, requests_mock):
    requests_mock.get(TAGS, exc=requests.exceptions.ConnectionError("rechazada"))

    with pytest.raises(OllamaNoDisponible, match="make llm-up"):
        cliente.comprobar()


# --- generar_indices ----------------------------------------------------------------------------


def test_generar_indices_devuelve_un_indice_por_dia_en_orden_y_envia_la_peticion_esperada(cliente, requests_mock):
    requests_mock.post(CHAT, **respuesta())

    indices = cliente.generar_indices("PROMPT", DIAS)

    assert [(i.fecha, i.indice, i.motivo) for i in indices] == [(d, 1.2, f"día {d.day}") for d in DIAS]
    cuerpo = requests_mock.last_request.json()
    assert cuerpo["model"] == "phi4" and cuerpo["stream"] is True  # en flujo: permite mostrar el progreso
    assert cuerpo["options"] == {"temperature": 0, "seed": 7, "num_ctx": 1024}
    assert cuerpo["format"]["properties"]["dias"]["minItems"] == 3
    assert cuerpo["messages"] == [{"role": "user", "content": "PROMPT"}]


def test_se_reintenta_con_el_error_como_aviso_y_sin_reenviar_la_respuesta_anterior(cliente, requests_mock):
    requests_mock.post(CHAT, [respuesta("{esto no es json"), respuesta()])

    indices = cliente.generar_indices("PROMPT", DIAS)

    assert len(indices) == 3 and requests_mock.call_count == 2
    mensajes = requests_mock.last_request.json()["messages"]
    assert len(mensajes) == 1 and mensajes[0]["role"] == "user"  # no se acumula contexto
    assert mensajes[0]["content"].startswith("PROMPT") and "AVISO" in mensajes[0]["content"]
    assert "JSON válido" in mensajes[0]["content"]


def test_tras_agotar_los_reintentos_falla_diciendo_que_dias_faltan(cliente, requests_mock):
    requests_mock.post(CHAT, [respuesta(contenido_ok(DIAS[:2]))] * 3)

    with pytest.raises(RespuestaInvalida, match="faltan.*2026-10-03") as error:
        cliente.generar_indices("PROMPT", DIAS)

    assert requests_mock.call_count == 3  # 1 intento + 2 reintentos
    assert "3 intentos" in str(error.value)


@pytest.mark.parametrize("fecha", ["2026-10-1", "2026-10-01T00:00:00", "20261001", "01/10/2026", 20261001, None])
def test_las_fechas_deben_ser_iso_exactas(cliente, requests_mock, fecha):
    cuerpo = {"dias": [entrada(fecha)] + [entrada(d.isoformat()) for d in DIAS[1:]]}
    requests_mock.post(CHAT, **respuesta(json.dumps(cuerpo)))

    with pytest.raises(RespuestaInvalida, match="no ISO"):
        cliente.generar_indices("PROMPT", DIAS, reintentos=0)


def test_una_fecha_inexistente_se_rechaza(cliente, requests_mock):
    cuerpo = {"dias": [entrada("2026-02-30")] + [entrada(d.isoformat()) for d in DIAS[1:]]}
    requests_mock.post(CHAT, **respuesta(json.dumps(cuerpo)))

    with pytest.raises(RespuestaInvalida, match="inexistente"):
        cliente.generar_indices("PROMPT", DIAS, reintentos=0)


def test_una_fecha_repetida_se_rechaza(cliente, requests_mock):
    cuerpo = {"dias": [entrada(d.isoformat()) for d in [DIAS[0], DIAS[0], DIAS[1], DIAS[2]]]}
    requests_mock.post(CHAT, **respuesta(json.dumps(cuerpo)))

    with pytest.raises(RespuestaInvalida, match="duplicad.*2026-10-01"):
        cliente.generar_indices("PROMPT", DIAS, reintentos=0)


def test_una_fecha_de_otro_trimestre_se_rechaza(cliente, requests_mock):
    cuerpo = {"dias": [entrada(d.isoformat()) for d in DIAS[:2]] + [entrada("2027-01-01")]}
    requests_mock.post(CHAT, **respuesta(json.dumps(cuerpo)))

    with pytest.raises(RespuestaInvalida, match="sobran.*2027-01-01"):
        cliente.generar_indices("PROMPT", DIAS, reintentos=0)


@pytest.mark.parametrize("indice", [0.0, 0.19, 3.01, -1, "1.2", None, True, float("inf")])
def test_el_indice_debe_ser_un_numero_dentro_de_los_limites(cliente, requests_mock, indice):
    cuerpo = {"dias": [entrada(DIAS[0].isoformat(), indice)] + [entrada(d.isoformat()) for d in DIAS[1:]]}
    requests_mock.post(CHAT, **respuesta(json.dumps(cuerpo)))

    with pytest.raises(RespuestaInvalida, match="índice"):
        cliente.generar_indices("PROMPT", DIAS, reintentos=0)


def test_el_motivo_se_limpia_a_una_linea_y_se_recorta(cliente, requests_mock):
    largo = "texto\n con   saltos " * 40
    cuerpo = {"dias": [entrada(d.isoformat(), motivo=largo) for d in DIAS]}
    requests_mock.post(CHAT, **respuesta(json.dumps(cuerpo)))

    motivo = cliente.generar_indices("PROMPT", DIAS)[0].motivo

    assert "\n" not in motivo and "  " not in motivo and len(motivo) <= 120


def test_un_motivo_ausente_no_invalida_la_respuesta(cliente, requests_mock):
    cuerpo = {"dias": [{"fecha": d.isoformat(), "indice": 1.0} for d in DIAS]}
    requests_mock.post(CHAT, **respuesta(json.dumps(cuerpo)))

    assert [i.motivo for i in cliente.generar_indices("PROMPT", DIAS)] == ["", "", ""]


# --- fallos de transporte y de contexto ---------------------------------------------------------


def test_una_respuesta_cortada_por_longitud_falla_sin_reintentar(cliente, requests_mock):
    requests_mock.post(CHAT, **respuesta('{"dias": [', done_reason="length"))

    with pytest.raises(ContextoInsuficiente, match="OLLAMA_NUM_CTX"):
        cliente.generar_indices("PROMPT", DIAS)

    assert requests_mock.call_count == 1  # reintentar daría exactamente lo mismo


def test_un_prompt_que_llena_el_contexto_se_considera_truncado(cliente, requests_mock):
    requests_mock.post(CHAT, **respuesta(prompt_eval_count=1024))

    with pytest.raises(ContextoInsuficiente, match="truncado"):
        cliente.generar_indices("PROMPT", DIAS)


def test_la_peticion_desactiva_el_desplazamiento_y_el_truncado_silenciosos(cliente, requests_mock):
    # Medido en Ollama 0.13.3: por defecto desplaza el contexto (done_reason 'stop' aunque la respuesta no quepa)
    # y trunca el prompt sin avisar; con shift/truncate a false avisa con 'length' y con HTTP 400.
    requests_mock.post(CHAT, **respuesta())

    cliente.generar_indices("PROMPT", DIAS)

    cuerpo = requests_mock.last_request.json()
    assert cuerpo["shift"] is False and cuerpo["truncate"] is False


def test_http_400_por_longitud_de_entrada_es_contexto_insuficiente_y_no_se_reintenta(cliente, requests_mock):
    requests_mock.post(CHAT, status_code=400, json={"error": "the input length exceeds the context length"})

    with pytest.raises(ContextoInsuficiente, match="OLLAMA_NUM_CTX"):
        cliente.generar_indices("PROMPT", DIAS)

    assert requests_mock.call_count == 1


def test_un_400_que_no_es_de_contexto_sigue_siendo_un_error_generico(cliente, requests_mock):
    requests_mock.post(CHAT, status_code=400, json={"error": "invalid format"})

    with pytest.raises(OllamaError, match="400.*invalid format") as error:
        cliente.generar_indices("PROMPT", DIAS)

    assert not isinstance(error.value, ContextoInsuficiente)


def test_prompt_mas_respuesta_que_llenan_el_contexto_aunque_diga_stop_es_contexto_insuficiente(cliente, requests_mock):
    # Red de seguridad para versiones de Ollama que ignoren shift=false: el contexto se llenó y se desplazó.
    requests_mock.post(CHAT, **respuesta(prompt_eval_count=800, eval_count=300))  # 1.100 >= num_ctx 1.024

    with pytest.raises(ContextoInsuficiente, match="desplaz"):
        cliente.generar_indices("PROMPT", DIAS)

    assert requests_mock.call_count == 1


def _flujo(contenido, partes=8, **final):
    """La respuesta de Ollama en flujo: un JSON por línea, con el contenido troceado y un último mensaje `done`."""
    paso = max(1, len(contenido) // partes)
    trozos = [contenido[i : i + paso] for i in range(0, len(contenido), paso)]
    lineas = [json.dumps({"message": {"role": "assistant", "content": t}, "done": False}) for t in trozos]
    lineas.append(json.dumps({"message": {"role": "assistant", "content": ""}, "done": True, "done_reason": "stop",
                              "prompt_eval_count": 100, "eval_count": 50, "eval_duration": 2_000_000_000, **final}))
    return {"text": "\n".join(lineas)}


def test_la_respuesta_en_flujo_se_recompone_y_se_muestra_el_progreso(requests_mock):
    dias = [date(2026, 10, 1) + timedelta(days=i) for i in range(20)]
    mensajes = []
    cliente = OllamaClient(OllamaSettings(url=URL, modelo="phi4", num_ctx=100_000, timeout_s=5, seed=7), imprimir=mensajes.append)
    requests_mock.post(CHAT, **_flujo(contenido_ok(dias), partes=40))

    indices = cliente.generar_indices("PROMPT", dias)

    assert [i.fecha for i in indices] == dias
    assert mensajes[0].startswith("Pidiendo a phi4 el índice de 20 días")
    progreso = [m for m in mensajes if "/20 días" in m]
    hechos = [int(m.split("/")[0]) for m in progreso]
    assert len(progreso) >= 3 and hechos == sorted(hechos) and hechos[-1] == 20  # sube y termina en el total
    assert "tok/s" in progreso[-1]


def test_un_error_a_mitad_del_flujo_se_traduce_a_un_error_de_ollama(cliente, requests_mock):
    lineas = [json.dumps({"message": {"content": '{"dias": ['}, "done": False}), json.dumps({"error": "CUDA out of memory"})]
    requests_mock.post(CHAT, text="\n".join(lineas))

    with pytest.raises(OllamaError, match="CUDA out of memory"):
        cliente.generar_indices("PROMPT", DIAS)


def test_un_flujo_cortado_antes_del_done_falla_con_un_mensaje_claro(cliente, requests_mock):
    requests_mock.post(CHAT, text=json.dumps({"message": {"content": '{"dias": ['}, "done": False}))

    with pytest.raises(OllamaError, match="se cortó"):
        cliente.generar_indices("PROMPT", DIAS)


def test_http_404_es_un_modelo_no_descargado(cliente, requests_mock):
    requests_mock.post(CHAT, status_code=404, json={"error": "model 'phi4' not found"})

    with pytest.raises(ModeloNoDescargado, match="ollama pull phi4"):
        cliente.generar_indices("PROMPT", DIAS)


def test_otro_error_http_incluye_el_cuerpo(cliente, requests_mock):
    requests_mock.post(CHAT, status_code=500, text="unable to create sampling context")

    with pytest.raises(OllamaError, match="500.*sampling context"):
        cliente.generar_indices("PROMPT", DIAS)


def test_el_timeout_dice_que_variable_subir(cliente, requests_mock):
    requests_mock.post(CHAT, exc=requests.exceptions.ReadTimeout)

    with pytest.raises(OllamaError, match="OLLAMA_TIMEOUT_S"):
        cliente.generar_indices("PROMPT", DIAS)


class _FlujoQueSeCuelga:
    """El `raw` de una respuesta cuyo servidor se queda mudo a mitad del flujo: urllib3 lanza ReadTimeoutError al leer."""

    status = 200
    reason = "OK"
    headers = {"Content-Type": "application/x-ndjson"}

    def stream(self, *_args, **_kwargs):
        yield (json.dumps({"message": {"content": '{"dias": ['}, "done": False}) + "\n").encode()
        raise ReadTimeoutError(None, CHAT, "Read timed out.")

    def release_conn(self):
        pass

    def close(self):
        pass


def test_un_timeout_a_mitad_del_flujo_dice_que_variable_subir_y_no_que_levante_ollama(cliente, monkeypatch):
    # requests envuelve ese ReadTimeoutError en un ConnectionError (no en un Timeout): con las cabeceras ya recibidas,
    # el caso habitual de un modelo lento es este, y el consejo de `make llm-up` sería el equivocado.
    colgada = requests.Response()
    colgada.status_code = 200
    colgada.raw = _FlujoQueSeCuelga()
    monkeypatch.setattr(cliente._http, "post", lambda *_a, **_k: colgada)

    with pytest.raises(OllamaError, match="OLLAMA_TIMEOUT_S") as error:
        cliente.generar_indices("PROMPT", DIAS)

    assert not isinstance(error.value, OllamaNoDisponible)


def test_sin_conexion_dice_como_levantar_ollama(cliente, requests_mock):
    requests_mock.post(CHAT, exc=requests.exceptions.ConnectionError("rechazada"))

    with pytest.raises(OllamaNoDisponible, match="make llm-up"):
        cliente.generar_indices("PROMPT", DIAS)
