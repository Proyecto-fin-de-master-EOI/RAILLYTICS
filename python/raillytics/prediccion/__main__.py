"""CLI de la predicción diaria de demanda del corredor AVE Madrid–Barcelona.

Uso:  python -m raillytics.prediccion --trimestre 2026-T4 [--prompt demanda_v2]
                                       [--total-esperado N] [--solo-nivel] [--mostrar-prompt]
      (o:  make 06_prediccion TRIMESTRE=2026-T4)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Mapping, Sequence

from dotenv import find_dotenv, load_dotenv

from raillytics.calidad.registro import QualityGateError
from raillytics.prediccion.entradas import EntradaError
from raillytics.prediccion.ollama import OllamaError
from raillytics.prediccion.servicio import VERSION_PROMPT_POR_DEFECTO, conectar, crear_cliente, ejecutar
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.lake import LakeLayout


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
    parser.add_argument(
        "--prompt", default=VERSION_PROMPT_POR_DEFECTO,
        help=f"versión de la plantilla en config/prompts (por defecto {VERSION_PROMPT_POR_DEFECTO})",
    )
    parser.add_argument("--total-esperado", type=_entero_positivo, help="fija el total del trimestre a mano en vez de calcularlo")
    parser.add_argument("--solo-nivel", action="store_true", help="calcula e imprime el total esperado sin llamar al LLM")
    parser.add_argument(
        "--mostrar-prompt", action="store_true", help="imprime el prompt exacto que se enviaría al LLM y termina (no llama al LLM)"
    )
    parser.add_argument(
        "--sin-cache", action="store_true", help="no usa la caché de resultados del LLM: genera de nuevo aunque ya hubiera una respuesta"
    )
    return parser.parse_args(argv)


def _consola_tolerante() -> None:
    """Un carácter que la consola no codifica (cp1252 en Windows con la salida redirigida) no debe romper la ejecución."""
    for flujo in (sys.stdout, sys.stderr):
        if hasattr(flujo, "reconfigure"):
            flujo.reconfigure(errors="replace")


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    _consola_tolerante()
    if env is None:
        load_dotenv(find_dotenv(usecwd=True))
        env = os.environ
    args = parsear(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    layout = LakeLayout.from_env(env)
    try:
        con = conectar(layout, env)
        cliente = None if args.solo_nivel or args.mostrar_prompt else crear_cliente(env, usar_cache=not args.sin_cache)
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
