"""El Makefile no debe depender de variables que el entorno de Windows ya define (PROMPT en cmd.exe)."""
import os
import shutil
import subprocess
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="necesita GNU make")


def _receta(**entorno):
    resultado = subprocess.run(
        ["make", "-n", "06_prediccion", "TRIMESTRE=2026-T4"],
        cwd=RAIZ, env=dict(os.environ, **entorno), capture_output=True, text=True,
    )
    return resultado.stdout


def test_la_plantilla_por_defecto_resiste_la_variable_PROMPT_de_cmd_exe():
    assert "--prompt demanda_v2" in _receta(PROMPT="$P$G")


def test_la_plantilla_se_elige_con_PRED_PROMPT():
    assert "--prompt demanda_v3" in _receta(PRED_PROMPT="demanda_v3")
