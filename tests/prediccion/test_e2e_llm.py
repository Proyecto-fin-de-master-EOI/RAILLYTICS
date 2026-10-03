"""Extremo a extremo con un Ollama REAL. No entra en `make test` (marcador `llm`).

Requisitos: `make llm-up` (o un Ollama local apuntado con OLLAMA_URL) y el modelo descargado.
Ejecutar:   .venv/bin/python -m pytest -m llm -v -s
Usa la plantilla por defecto: en modo eventos (eventos_vN) el LLM solo valora el día con evento y tarda segundos; con
una demanda_vN genera ~4.000 tokens y tarda minutos. Valida el contrato de salida, no la calidad del reparto.
"""
import os
from datetime import datetime
from pathlib import Path

import pytest

from raillytics.prediccion.ollama import OllamaClient, OllamaError, OllamaSettings
from raillytics.prediccion.servicio import VERSION_PROMPT_POR_DEFECTO, ejecutar
from raillytics.prediccion.trimestre import Trimestre

pytestmark = pytest.mark.llm

RAIZ = Path(__file__).resolve().parents[2]


def test_prediccion_completa_con_ollama_real(entorno):
    cliente = OllamaClient(OllamaSettings.from_env(os.environ))
    try:
        cliente.comprobar()
    except OllamaError as exc:
        pytest.skip(f"Ollama no disponible: {exc}")
    env = dict(entorno.env, PROMPTS_DIR=str(RAIZ / "config" / "prompts"))  # el prompt v1 de verdad

    resultado = ejecutar(
        Trimestre(2026, 4), version_prompt=VERSION_PROMPT_POR_DEFECTO, total_manual=None, solo_nivel=False, env=env,
        layout=entorno.layout, con=entorno.con, cliente=cliente, ahora=datetime(2026, 10, 1, 16, 51, 0), imprimir=print,
    )

    df = resultado.dataframe
    assert resultado.ruta.is_file() and len(df) == 92
    assert df["viajeros_previstos"].sum() == 1_320_000
    assert df["indice"].between(0.2, 3.0).all()
