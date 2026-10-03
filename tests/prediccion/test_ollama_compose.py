"""El Ollama de Docker debe guardar los modelos en un volumen persistente: se descargan UNA sola vez."""
import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml

RAIZ = Path(__file__).resolve().parents[2]
COMPOSE = yaml.safe_load((RAIZ / "docker" / "docker-compose.yml").read_text(encoding="utf-8"))
MAKEFILE = (RAIZ / "Makefile").read_text(encoding="utf-8")


def _receta(objetivo):
    return re.search(rf"^{objetivo}:\n((?:\t.*\n)+)", MAKEFILE, flags=re.M).group(1)


def test_ollama_guarda_los_modelos_en_un_volumen_con_nombre_declarado():
    montajes = COMPOSE["services"]["ollama"]["volumes"]

    # Sin OLLAMA_MODELS_DIR, el origen es el volumen con nombre `ollama_data` (no un volumen anónimo).
    assert "${OLLAMA_MODELS_DIR:-ollama_data}:/root/.ollama" in montajes
    assert "ollama_data" in COMPOSE["volumes"]


@pytest.mark.parametrize("objetivo", ["llm-up", "llm-down"])
def test_los_targets_llm_nunca_borran_volumenes(objetivo):
    receta = _receta(objetivo)

    assert not re.search(r"(^|\s)(-v|--volumes)(\s|$)", receta)
    assert " down" not in receta  # llm-down usa `rm -sf` solo sobre los contenedores ollama*


@pytest.mark.skipif(os.name == "nt", reason="ejecuta el entrypoint con /bin/sh y un ollama simulado")
@pytest.mark.parametrize("modelo_presente", [True, False])
def test_el_init_solo_descarga_cuando_el_modelo_no_esta_y_lo_dice(tmp_path, modelo_presente):
    llamadas = tmp_path / "llamadas.txt"
    falso = tmp_path / "ollama"
    falso.write_text(
        f'#!/bin/sh\necho "$1" >> "{llamadas}"\n[ "$1" = show ] && exit {0 if modelo_presente else 1}\nexit 0\n',
        encoding="utf-8",
    )
    falso.chmod(0o755)
    script = COMPOSE["services"]["ollama-init"]["entrypoint"][2].replace("$$", "$")  # compose convierte $$ en $

    salida = subprocess.run(
        ["/bin/sh", "-c", script],
        env={"PATH": f"{tmp_path}:/usr/bin:/bin", "OLLAMA_MODEL": "mistral-nemo"},
        capture_output=True, text=True,
    )

    assert salida.returncode == 0
    assert ("pull" in llamadas.read_text().split()) is (not modelo_presente)
    assert ("ya descargado" in salida.stdout) is modelo_presente


def test_airflow_ve_ollama_en_la_red_de_compose_y_las_rutas_montadas_para_la_prediccion():
    entorno = COMPOSE["x-airflow-common"]["environment"]

    assert entorno["OLLAMA_URL"] == "http://ollama:11434"
    assert entorno["PREDICCION_CONFIG"] == "/opt/airflow/raillytics_config/prediccion.yml"
    assert entorno["PROMPTS_DIR"] == "/opt/airflow/raillytics_config/prompts"
    assert entorno["REGLAS_DEMANDA"] == "/opt/airflow/raillytics_config/reglas_demanda.yml"  # modo eventos del DAG
    # los CSV van a un directorio del repo que está en git (no a data/, que se ignora), montado desde el anfitrión
    assert entorno["PREDICCIONES_ROOT"] == "/opt/airflow/resultados/predicciones"
    assert "../resultados/predicciones:/opt/airflow/resultados/predicciones" in COMPOSE["x-airflow-common"]["volumes"]
    # la caché de resultados del LLM vive en data/ (montado), así que sobrevive a recrear el contenedor
    assert entorno["PREDICCION_CACHE_DIR"] == "/opt/airflow/raillytics_data/cache/prediccion"


def test_ollama_arranca_con_flash_attention_y_kv_cache_comprimida_por_defecto_y_se_puede_cambiar():
    # Medido (RTX 2080 Ti, mistral-nemo, 92 días): 51 tok/s frente a 21 y el modelo cabe entero en la GPU.
    entorno = COMPOSE["services"]["ollama"]["environment"]

    assert entorno["OLLAMA_FLASH_ATTENTION"] == "${OLLAMA_FLASH_ATTENTION:-1}"
    assert entorno["OLLAMA_KV_CACHE_TYPE"] == "${OLLAMA_KV_CACHE_TYPE:-q8_0}"
