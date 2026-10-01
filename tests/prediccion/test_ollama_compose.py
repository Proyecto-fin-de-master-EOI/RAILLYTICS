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
