"""El Makefile no debe depender de variables que el entorno de Windows ya define (PROMPT en cmd.exe)."""
import json
import os
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="necesita GNU make")


def _make_n(objetivo, *argumentos, **entorno):
    resultado = subprocess.run(
        ["make", "-n", objetivo, *argumentos],
        cwd=RAIZ, env=dict(os.environ, **entorno), capture_output=True, text=True,
    )
    return resultado.stdout


def _receta(**entorno):
    return _make_n("06_prediccion", "TRIMESTRE=2026-T4", **entorno)


def test_la_plantilla_por_defecto_resiste_la_variable_PROMPT_de_cmd_exe():
    assert "--prompt demanda_v2" in _receta(PROMPT="$P$G")


def test_la_plantilla_se_elige_con_PRED_PROMPT():
    assert "--prompt demanda_v3" in _receta(PRED_PROMPT="demanda_v3")


def _conf_del_dag(*argumentos):
    salida = _make_n("00_ingest", *argumentos)
    linea = next(l for l in salida.splitlines() if "dags trigger" in l)
    return salida, json.loads(shlex.split(linea)[-1])  # el último argumento es el JSON de --conf


def test_00_ingest_levanta_ollama_antes_de_disparar_el_dag():
    salida, _ = _conf_del_dag()

    assert salida.index("up -d --wait ollama") < salida.index("run --rm ollama-init") < salida.index("airflow dags trigger ingesta_data_sources")


def test_00_ingest_pide_la_prediccion_al_dag_con_el_trimestre_y_la_plantilla():
    _, conf = _conf_del_dag("TRIMESTRE=2026-T4", "PRED_PROMPT=demanda_v3")

    assert conf == {"predecir": True, "trimestre": "2026-T4", "prompt": "demanda_v3"}


def test_00_ingest_sin_trimestre_deja_que_el_dag_use_el_trimestre_en_curso():
    _, conf = _conf_del_dag()

    assert conf == {"predecir": True, "trimestre": "", "prompt": "demanda_v2"}


def test_el_dag_lee_las_mismas_claves_de_conf_que_envia_el_makefile():
    dag = (RAIZ / "dags" / "ingesta_data_sources.py").read_text(encoding="utf-8")

    for clave in ("predecir", "trimestre", "prompt"):
        assert f'conf.get("{clave}")' in dag
    assert 'trigger_rule="all_done"' in dag  # que falle una descarga no impide predecir con lo que haya
