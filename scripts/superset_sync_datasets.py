"""Sincroniza con Superset el SQL y la descripción de los datasets que YA existen.

**El problema que resuelve.** `superset import-dashboards` identifica los activos por uuid y conserva
los que ya están, así que cambiar el YAML de un dataset existente **no tiene ningún efecto**. Hasta
ahora la única salida era borrarlo en la UI, lo que rompe de paso los dashboards que lo usan. Con eso
el repositorio dejaba de poder recrear el cuadro de mandos: el YAML decía una cosa y Superset mostraba
otra, sin avisar.

Esto lo hace por la API REST, que es el camino soportado, y **solo toca lo que ha cambiado**: compara
el `sql`, la `description`, las columnas y las métricas de cada YAML con lo que tiene Superset y manda
un PUT únicamente de los datasets que difieren.

**Las columnas y métricas se MEZCLAN, no se reemplazan**: se añaden las que faltan y se actualizan las
que cambiaron, pero no se borra lo que esté en Superset y no esté en el YAML. Borrar una métrica deja
sin datos a cualquier gráfica que la use, y eso no se hace de corrido: se avisa por pantalla y se
decide a mano. Añadir una columna al SQL sin esto no sirve de nada —Superset no la conoce y la gráfica
que la use falla con «Columns missing in dataset»—.

Uso:
    python scripts/superset_sync_datasets.py                 # todos los que difieran
    python scripts/superset_sync_datasets.py kpi_plan        # solo esos
    python scripts/superset_sync_datasets.py --dry-run       # dice qué cambiaría y no cambia nada

Lanzarlo DESPUÉS de `make 06_superset-import`: el import crea los que faltan, esto actualiza los que
ya estaban.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import yaml
from dotenv import find_dotenv, load_dotenv

RAIZ = Path(__file__).resolve().parent.parent
DATASETS = RAIZ / "dashboards" / "superset" / "raillytics_gold" / "datasets" / "Raillytics_Gold_DuckDB"
CAMPOS = ("sql", "description")

# Lo que se manda de cada columna y de cada métrica. El resto de lo que devuelve la API (id, uuid,
# fechas, `type_generic`) o no se puede enviar o lo pone Superset.
CAMPOS_COLUMNA = ("column_name", "verbose_name", "description", "type", "expression",
                  "is_dttm", "is_active", "groupby", "filterable", "advanced_data_type",
                  "python_date_format", "extra")
CAMPOS_METRICA = ("metric_name", "verbose_name", "description", "expression", "metric_type",
                  "d3format", "warning_text", "extra")


class SupersetError(RuntimeError):
    """La API ha respondido algo que no se puede continuar; el mensaje dice qué."""


class Api:
    def __init__(self, base: str, usuario: str, clave: str) -> None:
        self.base = base.rstrip("/")
        self.token = ""
        self.csrf = ""
        self.cookie = ""
        self._entrar(usuario, clave)

    def _pedir(self, metodo: str, ruta: str, cuerpo: dict | None = None) -> tuple[dict, str]:
        req = urllib.request.Request(self.base + ruta, method=metodo)
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        if self.csrf:
            req.add_header("X-CSRFToken", self.csrf)
        if self.cookie:
            req.add_header("Cookie", self.cookie)
        datos = json.dumps(cuerpo).encode("utf-8") if cuerpo is not None else None
        try:
            with urllib.request.urlopen(req, datos, timeout=60) as r:
                return json.loads(r.read() or b"{}"), r.headers.get("Set-Cookie", "")
        except urllib.error.HTTPError as e:
            detalle = e.read().decode("utf-8", "replace")[:500]
            raise SupersetError(f"{metodo} {ruta} -> HTTP {e.code}: {detalle}") from e
        except urllib.error.URLError as e:
            raise SupersetError(f"no se puede hablar con Superset en {self.base} ({e.reason}). "
                                "¿Está levantado? `make up`") from e

    def _entrar(self, usuario: str, clave: str) -> None:
        sesion, _ = self._pedir("POST", "/api/v1/security/login",
                                {"username": usuario, "password": clave, "provider": "db", "refresh": True})
        self.token = sesion["access_token"]
        # El PUT necesita el token CSRF y la cookie de sesión que viene con él.
        csrf, cookie = self._pedir("GET", "/api/v1/security/csrf_token/")
        self.csrf, self.cookie = csrf["result"], cookie

    def buscar_dataset(self, nombre: str) -> dict[str, Any] | None:
        filtro = json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": nombre}]})
        respuesta, _ = self._pedir("GET", f"/api/v1/dataset/?q={urllib.parse.quote(filtro)}")
        for fila in respuesta.get("result", []):
            if fila.get("table_name") == nombre:
                detalle, _ = self._pedir("GET", f"/api/v1/dataset/{fila['id']}")
                return {"id": fila["id"], **detalle.get("result", {})}
        return None

    def actualizar_dataset(self, ident: int, cambios: dict[str, Any]) -> None:
        self._pedir("PUT", f"/api/v1/dataset/{ident}", cambios)


def _normaliza(d: dict, campos: tuple[str, ...]) -> dict:
    return {c: d.get(c) for c in campos}


def mezclar(remotos: list[dict], locales: list[dict], clave: str, campos: tuple[str, ...]
            ) -> tuple[list[dict] | None, list[str], list[str]]:
    """Las columnas (o métricas) a mandar, lo que se añade o cambia, y lo que sobra en Superset.

    Devuelve `None` en la lista si no hay nada que cambiar, para no mandar un PUT de más. Lo que está
    en Superset y no en el YAML se conserva y se devuelve aparte: borrarlo dejaría sin datos a las
    gráficas que lo usen, así que se avisa en vez de hacerlo.
    """
    por_nombre = {r[clave]: r for r in remotos}
    salida, tocados = [], []
    for local in locales:
        nombre = local[clave]
        quiero = _normaliza(local, campos)
        remoto = por_nombre.get(nombre)
        if remoto is None:
            salida.append(quiero)
            tocados.append(f"+{nombre}")
            continue
        fila = {**quiero, "id": remoto["id"]}
        salida.append(fila)
        if _normaliza(remoto, campos) != quiero:
            tocados.append(f"~{nombre}")
    sobran = sorted(set(por_nombre) - {l[clave] for l in locales})
    # Lo que sobra viaja tal cual para no perderlo en el PUT.
    salida += [{**_normaliza(por_nombre[n], campos), "id": por_nombre[n]["id"]} for n in sobran]
    return (salida if tocados else None), tocados, sobran


def yamls(nombres: list[str]) -> list[Path]:
    if nombres:
        rutas = [DATASETS / f"{n}.yaml" for n in nombres]
        faltan = [r.name for r in rutas if not r.exists()]
        if faltan:
            raise SupersetError(f"no hay YAML para: {', '.join(faltan)} (en {DATASETS})")
        return rutas
    return sorted(DATASETS.glob("*.yaml"))


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    import os

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("datasets", nargs="*", help="nombres a sincronizar (por defecto, todos)")
    p.add_argument("--dry-run", action="store_true", help="solo dice qué cambiaría")
    p.add_argument("--base", default=f"http://localhost:{os.environ.get('SUPERSET_PORT', '8088')}")
    args = p.parse_args(argv)

    usuario = os.environ.get("SUPERSET_ADMIN_USER")
    clave = os.environ.get("SUPERSET_ADMIN_PASSWORD")
    if not usuario or not clave:
        print("faltan SUPERSET_ADMIN_USER y SUPERSET_ADMIN_PASSWORD en el .env", file=sys.stderr)
        return 2

    try:
        rutas = yamls(args.datasets)
        api = None if args.dry_run else Api(args.base, usuario, clave)
        if args.dry_run:
            api = Api(args.base, usuario, clave)   # leer sí, escribir no
        cambiados = iguales = nuevos = 0
        for ruta in rutas:
            local = yaml.safe_load(ruta.read_text(encoding="utf-8")) or {}
            nombre = local.get("table_name") or ruta.stem
            remoto = api.buscar_dataset(nombre)
            if remoto is None:
                print(f"  {nombre}: no existe en Superset todavía (lo crea `make 06_superset-import`)")
                nuevos += 1
                continue
            cambios = {c: local.get(c) for c in CAMPOS if (local.get(c) or "") != (remoto.get(c) or "")}
            columnas, toca_col, sobran_col = mezclar(
                remoto.get("columns") or [], local.get("columns") or [], "column_name", CAMPOS_COLUMNA)
            metricas, toca_met, sobran_met = mezclar(
                remoto.get("metrics") or [], local.get("metrics") or [], "metric_name", CAMPOS_METRICA)
            if columnas is not None:
                cambios["columns"] = columnas
            if metricas is not None:
                cambios["metrics"] = metricas
            for que, sobra in (("columnas", sobran_col), ("métricas", sobran_met)):
                if sobra:
                    print(f"  {nombre}: {que} en Superset que el YAML ya no declara, se CONSERVAN "
                          f"(bórralas a mano si de verdad sobran): {', '.join(sobra)}")
            if not cambios:
                iguales += 1
                continue
            detalle = [c for c in cambios if c not in ("columns", "metrics")]
            detalle += [f"columnas({', '.join(toca_col)})"] if toca_col else []
            detalle += [f"métricas({', '.join(toca_met)})"] if toca_met else []
            print(f"  {nombre} (id {remoto['id']}): cambia {', '.join(detalle)}")
            if not args.dry_run:
                api.actualizar_dataset(remoto["id"], cambios)
            cambiados += 1
    except SupersetError as e:
        print(f"✗ {e}", file=sys.stderr)
        return 1

    verbo = "cambiarían" if args.dry_run else "actualizados"
    print(f"  {cambiados} {verbo}, {iguales} ya estaban al día, {nuevos} sin crear")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
