"""CLI de la predicción diaria de demanda del corredor AVE Madrid–Barcelona.

Uso:  python -m raillytics.prediccion --trimestre 2026-T4 [--prompt demanda_v1]
                                       [--total-esperado N] [--solo-nivel] [--mostrar-prompt]
      (o:  make 06_prediccion TRIMESTRE=2026-T4)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Mapping, Sequence

import duckdb
from dotenv import find_dotenv, load_dotenv

from raillytics.calidad.registro import QualityGateError
from raillytics.prediccion.entradas import EntradaError, raiz_bronze
from raillytics.prediccion.ollama import OllamaClient, OllamaError, OllamaSettings
from raillytics.prediccion.servicio import ejecutar
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.lake import LakeLayout, S3Settings, connect, is_s3


def _trimestre(texto: str) -> Trimestre:
    try:
        return Trimestre.parse(texto)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _entero_positivo(texto: str) -> int:
    try:
        valor = int(texto)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{texto!r} no es un entero") from exc
    if valor <= 0:
        raise argparse.ArgumentTypeError("debe ser un entero positivo")
    return valor


def parsear(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m raillytics.prediccion",
        description="Predice la demanda diaria del corredor AVE Madrid–Barcelona para un trimestre con un LLM de Ollama.",
    )
    parser.add_argument("--trimestre", required=True, type=_trimestre, help="trimestre objetivo, p. ej. 2026-T4")
    parser.add_argument("--prompt", default="demanda_v1", help="versión de la plantilla en config/prompts (por defecto demanda_v1)")
    parser.add_argument("--total-esperado", type=_entero_positivo, help="fija el total del trimestre a mano en vez de calcularlo")
    parser.add_argument("--solo-nivel", action="store_true", help="calcula e imprime el total esperado sin llamar al LLM")
    parser.add_argument("--mostrar-prompt", action="store_true", help="imprime el prompt exacto que se envía al LLM")
    return parser.parse_args(argv)


def _conectar(layout: LakeLayout, env: Mapping[str, str]) -> duckdb.DuckDBPyConnection:
    usa_s3 = layout.uses_s3 or is_s3(raiz_bronze(env))
    return connect(S3Settings.from_env(env) if usa_s3 else None)


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    if env is None:
        load_dotenv(find_dotenv(usecwd=True))
        env = os.environ
    args = parsear(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    layout = LakeLayout.from_env(env)
    try:
        con = _conectar(layout, env)
        cliente = None if args.solo_nivel else OllamaClient(OllamaSettings.from_env(env))
        ejecutar(
            args.trimestre,
            version_prompt=args.prompt,
            total_manual=args.total_esperado,
            solo_nivel=args.solo_nivel,
            mostrar_prompt=args.mostrar_prompt,
            env=env,
            layout=layout,
            con=con,
            cliente=cliente,
        )
    except (EntradaError, OllamaError, QualityGateError, ValueError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
