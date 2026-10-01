"""Predicción diaria de demanda del corredor AVE Madrid–Barcelona con un LLM (Ollama).

El código fija el nivel del trimestre (total esperado) y el LLM solo reparte ese total entre
los días con un índice relativo. Se lanza con `make 06_prediccion TRIMESTRE=AAAA-Tn`.
"""
