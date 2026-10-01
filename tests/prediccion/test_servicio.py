import json
import shutil
from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest
import yaml

from raillytics.calidad.registro import QualityGateError
from raillytics.prediccion.entradas import EntradaError
from raillytics.prediccion.gates import TABLA
from raillytics.prediccion.publicacion import GOLD_TABLA
from raillytics.prediccion.nivel import NivelError
from raillytics.prediccion.normalizar import IndiceDia
from raillytics.prediccion.ollama import OllamaSettings, RespuestaInvalida
from raillytics.prediccion.prompt import PlantillaError
from raillytics.prediccion import servicio
from raillytics.prediccion.cache import CacheLLM, ClienteConCache
from raillytics.prediccion.ollama import OllamaClient
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


def _cargas(entorno, tabla=TABLA):
    return duckdb.connect().execute(
        f"SELECT proceso, capa, tabla, destino, filas, estado, error, parametros "
        f"FROM read_parquet('{entorno.layout.cargas_glob()}') WHERE tabla = '{tabla}'"
    ).fetchall()


def _gold(entorno):
    return duckdb.connect().execute(
        f"SELECT run_id, fecha, viajeros_previstos, total_esperado, datos_sinteticos, festivo, eventos "
        f"FROM read_parquet('{entorno.layout.gold_glob(GOLD_TABLA)}') ORDER BY run_id, fecha"
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
        "total_manual": None, "cache": "desactivada",
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


def test_avisa_cuando_las_consultas_leen_fuentes_sinteticas(entorno):
    shutil.copytree(entorno.silver / "festivos", entorno.silver / "muestra_festivos")
    config = yaml.safe_load(entorno.config.read_text(encoding="utf-8"))
    config["origenes"]["festivos"]["sql"] = "SELECT fecha, nombre FROM read_parquet('{silver}/muestra_festivos/*.parquet')"
    entorno.config.write_text(yaml.safe_dump(config), encoding="utf-8")

    _, salida = _lanzar(entorno, ClienteFalso())

    aviso = next(linea for linea in salida if linea.startswith("AVISO") and "SINTÉTICAS" in linea and "NO son reales" in linea)
    assert "festivos" in aviso and "trimestrales" not in aviso   # nombra solo los orígenes que leen la muestra


def test_no_avisa_de_datos_sinteticos_si_las_consultas_leen_fuentes_reales(entorno):
    _, salida = _lanzar(entorno, ClienteFalso())

    assert not any("SINTÉTICAS" in linea for linea in salida)


def test_mostrar_prompt_imprime_el_prompt_y_termina_sin_llamar_al_llm(entorno):
    cliente = ClienteFalso()

    resultado, salida = _lanzar(entorno, cliente, mostrar_prompt=True)

    assert cliente.comprobaciones == 0 and cliente.prompts == []  # ni siquiera se comprueba Ollama
    assert resultado.ruta is None and _csvs(entorno) == []
    assert not Path(entorno.layout.cargas_dir).exists()  # no queda carga registrada
    enviado = ClienteFalso()
    _lanzar(entorno, enviado)  # ejecución normal
    assert enviado.prompts[0] in salida  # lo mostrado es exactamente lo que luego se envía


def test_mostrar_prompt_no_necesita_cliente(entorno):
    resultado, salida = _lanzar(entorno, None, mostrar_prompt=True)

    assert resultado.ruta is None and any("1.320.000" in linea for linea in salida)


def test_la_salida_por_consola_cabe_en_cp1252(entorno):
    # Windows con la salida redirigida usa cp1252: un carácter fuera (p. ej. 'σ') rompía la ejecución tras escribir el CSV.
    _, salida = _lanzar(entorno, ClienteFalso())

    "\n".join(salida).encode("cp1252")


def test_si_el_llm_falla_no_se_escribe_nada_y_la_carga_queda_en_error(entorno):
    with pytest.raises(RespuestaInvalida):
        _lanzar(entorno, ClienteFalso(error=RespuestaInvalida("faltan días")))

    assert _csvs(entorno) == []
    (_, _, _, _, _, estado, error, _), = _cargas(entorno)
    assert estado == "error" and "faltan días" in error


def test_un_gate_bloqueante_impide_escribir_y_queda_registrado(entorno):
    extremos = [3.5] + [1.0] * 91  # un índice crudo por encima del límite duro de 3.0 (el cliente real ya lo rechazaría)

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


# --- punto de entrada del DAG de Airflow: todo sale del entorno del contenedor ---


@pytest.fixture
def desde_entorno(entorno, monkeypatch):
    """Sustituye Ollama y la conexión al lake; el resto (config, prompts, CSV) es el código real."""
    cliente = ClienteFalso()
    monkeypatch.setattr(servicio, "OllamaClient", lambda settings, **kwargs: cliente)
    monkeypatch.setattr(servicio, "conectar", lambda layout, env: entorno.con)
    env = dict(entorno.env, SILVER_ROOT=entorno.layout.silver_root, GOLD_ROOT=entorno.layout.gold_root, PREDICCION_CACHE="0")
    return entorno, cliente, env


def test_predecir_desde_entorno_usa_el_trimestre_en_curso_si_no_se_indica(desde_entorno):
    entorno, cliente, env = desde_entorno

    resultado = servicio.predecir_desde_entorno(None, date(2026, 10, 1), env, version_prompt="demanda_v1", imprimir=lambda _: None)

    assert resultado.ruta.parent.name == "2026-T4" and resultado.ruta.is_file()
    assert resultado.total_esperado == 1_320_000 and cliente.comprobaciones == 1


def test_predecir_desde_entorno_respeta_el_trimestre_pedido(desde_entorno):
    _, _, env = desde_entorno

    resultado = servicio.predecir_desde_entorno("2026-T4", date(2030, 1, 1), env, version_prompt="demanda_v1", imprimir=lambda _: None)

    assert resultado.ruta.parent.name == "2026-T4"


def test_predecir_desde_entorno_rechaza_un_trimestre_mal_escrito(desde_entorno):
    _, cliente, env = desde_entorno

    with pytest.raises(ValueError, match="AAAA-Tn"):
        servicio.predecir_desde_entorno("2026-Q4", date(2026, 10, 1), env, imprimir=lambda _: None)

    assert cliente.comprobaciones == 0


def test_la_plantilla_por_defecto_es_la_v2_y_se_puede_cambiar_con_el_entorno(desde_entorno):
    _, _, env = desde_entorno

    assert servicio.VERSION_PROMPT_POR_DEFECTO == "demanda_v2"
    with pytest.raises(PlantillaError, match="demanda_v2"):  # el lake de prueba solo tiene demanda_v1
        servicio.predecir_desde_entorno(None, date(2026, 10, 1), env, imprimir=lambda _: None)
    resultado = servicio.predecir_desde_entorno(
        None, date(2026, 10, 1), dict(env, PRED_PROMPT="demanda_v1"), imprimir=lambda _: None
    )
    assert "demanda_v1" in resultado.ruta.name


# --- caché de resultados del LLM ---


def test_crear_cliente_envuelve_a_ollama_en_la_cache_salvo_que_se_desactive(tmp_path):
    env = {"PREDICCION_CACHE_DIR": str(tmp_path / "cache")}

    cliente = servicio.crear_cliente(env)

    assert isinstance(cliente, ClienteConCache) and cliente.settings.modelo == "mistral-nemo"
    assert isinstance(servicio.crear_cliente(env, usar_cache=False), OllamaClient)
    for apagado in ("0", "false", "NO", "off"):
        assert isinstance(servicio.crear_cliente(dict(env, PREDICCION_CACHE=apagado)), OllamaClient)


def test_la_segunda_ejecucion_identica_sale_de_la_cache_y_queda_anotado_en_la_carga(entorno, tmp_path):
    real = ClienteFalso()
    cliente = ClienteConCache(real, CacheLLM(tmp_path / "cache"), lambda _: None)

    _lanzar(entorno, cliente, ahora=AHORA)
    segunda, salida = _lanzar(entorno, cliente, ahora=datetime(2026, 10, 1, 16, 52, 0))

    assert len(real.prompts) == 1  # el LLM solo se llamó en la primera
    assert segunda.ruta.is_file() and len(segunda.dataframe) == 92  # pero cada ejecución escribe su CSV
    caches = sorted(json.loads(fila[7])["cache"] for fila in _cargas(entorno))
    assert caches == ["acierto", "fallo"]


def test_sin_cache_la_carga_dice_que_estaba_desactivada(entorno):
    _lanzar(entorno, ClienteFalso())

    assert json.loads(_cargas(entorno)[0][7])["cache"] == "desactivada"


# --- publicación en Gold (para el dashboard de Superset) ---


def test_cada_ejecucion_se_publica_en_gold_y_queda_constancia_en_la_trazabilidad(entorno):
    resultado, _ = _lanzar(entorno, ClienteFalso())

    filas = _gold(entorno)
    assert len(filas) == 92 and {f[0] for f in filas} == {resultado.dataframe["run_id"].iloc[0]}
    assert sum(f[2] for f in filas) == resultado.total_esperado == filas[0][3]
    assert {f[4] for f in filas} == {False}  # las consultas de la fixture no leen fuentes sintéticas
    assert {f[5] for f in filas if f[5]} == {"Navidad", "Inmaculada"} and any(f[6] for f in filas)  # festivos y evento del lake de prueba
    (destino, filas_gold, estado), = [(f[3], f[4], f[5]) for f in _cargas(entorno, GOLD_TABLA)]
    assert destino.endswith(f"/{GOLD_TABLA}/{resultado.dataframe['run_id'].iloc[0]}.parquet") and (filas_gold, estado) == (92, "ok")


def test_las_predicciones_con_fuentes_sinteticas_quedan_marcadas_en_gold(entorno):
    shutil.copytree(entorno.silver / "festivos", entorno.silver / "muestra_festivos")
    config = yaml.safe_load(entorno.config.read_text(encoding="utf-8"))
    config["origenes"]["festivos"]["sql"] = "SELECT fecha, nombre FROM read_parquet('{silver}/muestra_festivos/*.parquet')"
    entorno.config.write_text(yaml.safe_dump(config), encoding="utf-8")

    _lanzar(entorno, ClienteFalso())

    assert {f[4] for f in _gold(entorno)} == {True}


def test_si_falla_la_publicacion_en_gold_no_se_escribe_el_csv(entorno, monkeypatch):
    def falla(*args, **kwargs):
        raise OSError("MinIO no responde")

    monkeypatch.setattr(servicio, "publicar_gold", falla)

    with pytest.raises(OSError, match="MinIO"):
        _lanzar(entorno, ClienteFalso())

    assert _csvs(entorno) == [] and _cargas(entorno)[0][5] == "error"
