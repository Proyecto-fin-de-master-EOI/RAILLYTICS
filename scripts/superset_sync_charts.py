"""Sincroniza con Superset los gráficos que YA existen (el hermano de `superset_sync_datasets.py`).

**El problema que resuelve.** `superset import-dashboards` identifica los activos por uuid y conserva
los que ya están: sobrescribe los dashboards —el layout, los anchos, qué gráfico va en qué fila— pero
**NO los gráficos**. Así que cambiar en el YAML el formato de un número, una métrica o el límite de
filas no tiene ningún efecto, y el repositorio y Superset se van separando sin avisar.

Eso no es teórico. El 2026-10-17 se renombraron columnas para hablar como el catálogo
(`viajeros_min`/`viajeros_max` -> `cota_inferior`/`cota_superior`, `version_prompt` -> `version`). Los
YAML de los gráficos se actualizaron; Superset se quedó con los viejos, y dos gráficos del panel
estuvieron un día entero dando «Referenced column not found» mientras el repositorio decía lo
correcto. Sin esto, la única salida era borrarlos en la UI.

Manda un PUT solo de los que difieren, y de cada uno únicamente estos campos:

    slice_name, viz_type, params, description, cache_timeout

`query_context` se manda vacío a propósito: es la consulta que Superset cachea al guardar un gráfico
en la UI y, si se deja la vieja, el gráfico seguiría pidiendo las columnas de antes aunque sus
`params` ya sean los nuevos. Vaciándolo, Superset la reconstruye en la siguiente carga.

**No borra nada.** Un gráfico que esté en Superset y no en el repositorio se deja estar y se avisa por
pantalla: puede ser de otra persona del equipo.

Uso:
    python scripts/superset_sync_charts.py                      # todos los que difieran
    python scripts/superset_sync_charts.py top_dias_demanda     # solo esos (por nombre de fichero)
    python scripts/superset_sync_charts.py --dry-run            # dice qué cambiaría y no cambia nada

Lanzarlo DESPUÉS de `make 06_superset-import`: el import crea los que faltan, esto actualiza los que
ya estaban.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
from pathlib import Path
from typing import Any

import yaml
from dotenv import find_dotenv, load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from superset_sync_datasets import Api, SupersetError  # noqa: E402  (comparte el cliente de la API)

RAIZ = Path(__file__).resolve().parent.parent
CHARTS = RAIZ / "dashboards" / "superset" / "raillytics_gold" / "charts"

# Lo que se compara y se manda. `params` va aparte porque en el YAML es un diccionario y la API lo
# quiere como cadena JSON.
CAMPOS = ("slice_name", "viz_type", "description", "cache_timeout")


def listar_graficos(api: Api) -> dict[str, dict[str, Any]]:
    """Todos los gráficos de Superset, por uuid. Se pagina porque son más de los que caben de una."""
    por_uuid: dict[str, dict[str, Any]] = {}
    pagina = 0
    while True:
        consulta = urllib.parse.quote(json.dumps({"page": pagina, "page_size": 100,
                                                  "columns": ["id", "uuid", "slice_name", "viz_type",
                                                              "description", "cache_timeout", "params"]}))
        respuesta, _ = api._pedir("GET", f"/api/v1/chart/?q={consulta}")
        filas = respuesta.get("result", [])
        for fila in filas:
            if fila.get("uuid"):
                por_uuid[fila["uuid"]] = fila
        if len(filas) < 100:
            return por_uuid
        pagina += 1


def diferencias(local: dict[str, Any], remoto: dict[str, Any]) -> list[str]:
    """Qué campos del YAML no coinciden con lo que tiene Superset."""
    cambios = [c for c in CAMPOS if (local.get(c) or None) != (remoto.get(c) or None)]
    # `params` se compara ya parseado: el orden de las claves en la cadena no significa nada.
    try:
        remotos = json.loads(remoto.get("params") or "{}")
    except json.JSONDecodeError:
        remotos = {}
    if (local.get("params") or {}) != remotos:
        cambios.append("params")
    return cambios


def yamls(nombres: list[str]) -> list[Path]:
    if nombres:
        rutas = [CHARTS / f"{n}.yaml" for n in nombres]
        faltan = [r.name for r in rutas if not r.exists()]
        if faltan:
            raise SupersetError(f"no hay YAML para: {', '.join(faltan)} (en {CHARTS})")
        return rutas
    return sorted(CHARTS.glob("*.yaml"))


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    parser = argparse.ArgumentParser(description="Sincroniza con Superset los gráficos del repositorio")
    parser.add_argument("nombres", nargs="*", help="nombres de fichero sin .yaml; vacío = todos")
    parser.add_argument("--dry-run", action="store_true", help="dice qué cambiaría y no cambia nada")
    args = parser.parse_args(argv)

    api = Api(os.environ.get("SUPERSET_URL", "http://localhost:8088"),
              os.environ.get("SUPERSET_USER", "admin"),
              os.environ.get("SUPERSET_PASSWORD", "admin"))
    remotos = listar_graficos(api)

    actualizados = aldia = sincrear = 0
    for ruta in yamls(args.nombres):
        local = yaml.safe_load(ruta.read_text(encoding="utf-8")) or {}
        remoto = remotos.get(local.get("uuid"))
        if remoto is None:
            print(f"  {ruta.stem}: no está en Superset todavía (lo crea `make 06_superset-import`)")
            sincrear += 1
            continue
        cambios = diferencias(local, remoto)
        if not cambios:
            aldia += 1
            continue
        print(f"  {ruta.stem} (id {remoto['id']}): cambia {', '.join(cambios)}")
        if not args.dry_run:
            cuerpo: dict[str, Any] = {c: local.get(c) for c in CAMPOS if local.get(c) is not None}
            cuerpo["params"] = json.dumps(local.get("params") or {}, ensure_ascii=False)
            # Vacía la consulta cacheada para que Superset la rehaga con los params nuevos.
            cuerpo["query_context"] = None
            api._pedir("PUT", f"/api/v1/chart/{remoto['id']}", cuerpo)
        actualizados += 1

    sobran = sorted(r["slice_name"] for u, r in remotos.items()
                    if u not in {(yaml.safe_load(p.read_text(encoding="utf-8")) or {}).get("uuid")
                                 for p in CHARTS.glob("*.yaml")})
    if sobran:
        print(f"  en Superset y no en el repositorio, se CONSERVAN: {', '.join(sobran[:10])}"
              + (" …" if len(sobran) > 10 else ""))
    print(f"{actualizados} actualizado(s), {aldia} ya estaban al día, {sincrear} sin crear"
          + (" (dry-run: no se ha tocado nada)" if args.dry_run else ""))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SupersetError as e:
        print(f"error: {e}", file=sys.stderr)
        raise SystemExit(1) from e
