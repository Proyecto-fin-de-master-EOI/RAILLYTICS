from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from raillytics.ingesta.formats import (
    CHECKS_DESCARGA,
    CLAVES_FUENTE,
    DESCARGADORES,
    MODOS_SILVER,
    OPCIONES_AUTH,
    OPCIONES_LECTURA,
    OPCIONES_POR_FORMATO,
    SUPPORTED_FORMATS,
)

_NOMBRE_TABLA = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class SilverTabla:
    """Una tabla Silver que `make 04_silver` construye a partir de la fuente (la lee DataSourceConfig.scala)."""

    tabla: str
    modo: str


@dataclass(frozen=True)
class DataSource:
    id: str
    name: str
    url: str
    format: str
    options: dict[str, str] = field(default_factory=dict)  # cómo se lee el fichero: delimiter y encoding en csv, rowTag en xml
    checks: dict[str, Any] = field(default_factory=dict)   # reglas de la descarga: min_bytes, min_filas, columnas (solo csv)
    silver: tuple[SilverTabla, ...] = ()                   # tablas Silver que se construyen desde esta fuente
    downloader: str = "http"                               # quién trae la fuente (ver downloaders.py); http = GET directo de `url`
    auth: dict[str, str] = field(default_factory=dict)     # {env: NOMBRE_VARIABLE, header: ...}; nunca el secreto


def load_sources(path: Path) -> list[DataSource]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    sources = [_parsear(entry) for entry in raw["sources"]]
    tablas = [t.tabla for s in sources for t in s.silver]
    repetidas = sorted({t for t in tablas if tablas.count(t) > 1})
    if repetidas:
        raise ValueError(f"Tablas Silver declaradas por más de una fuente: {', '.join(repetidas)}")
    return sources


def _parsear(entry: dict[str, Any]) -> DataSource:
    for clave in ("id", "name", "url", "format"):  # primero lo que falta (un typo en `id` se ve como «falta 'id'»), después lo que sobra
        if clave not in entry:
            raise ValueError(f"Campo requerido ausente en config/data_sources.yml: '{clave}'")
    fuente = entry["id"]
    desconocidas = sorted(set(entry) - CLAVES_FUENTE)
    if desconocidas:
        raise ValueError(
            f"Clave desconocida en la fuente '{fuente}': {', '.join(desconocidas)} (válidas: {', '.join(sorted(CLAVES_FUENTE))})"
        )
    fmt = entry["format"]
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(f"Formato no soportado: '{fmt}' (soportados: {sorted(SUPPORTED_FORMATS)})")
    options = _bloque(fuente, "options", entry, OPCIONES_LECTURA)
    # Cada formato admite sus propias opciones y solo las suyas; los que no admiten ninguna (json,
    # zip) se leen igual siempre, así que declarar `options` en ellos es un error de configuración.
    permitidas = OPCIONES_POR_FORMATO.get(fmt, set())
    impropias = sorted(set(options) - permitidas)
    if impropias:
        admite = f" (admite: {', '.join(sorted(permitidas))})" if permitidas else ""
        raise ValueError(f"Fuente '{fuente}': el formato '{fmt}' no admite {', '.join(impropias)} en 'options'{admite}")
    checks = _bloque(fuente, "checks", entry, CHECKS_DESCARGA)
    if checks and fmt != "csv":
        raise ValueError(f"Fuente '{fuente}': 'checks' solo se admite en fuentes csv (formato '{fmt}')")
    delimitador = options.get("delimiter")
    if delimitador is not None and len(str(delimitador)) != 1:
        raise ValueError(f"Fuente '{fuente}': options.delimiter debe ser un único carácter, no {delimitador!r}")
    if fmt == "xml" and not str(options.get("rowTag", "")).strip():
        raise ValueError(
            f"Fuente '{fuente}': una fuente xml necesita options.rowTag, el elemento que L2 trata como fila"
        )
    _validar_checks(fuente, checks)
    downloader = entry.get("downloader", "http")
    if downloader not in DESCARGADORES:
        raise ValueError(
            f"Fuente '{fuente}': downloader no soportado '{downloader}' (soportados: {sorted(DESCARGADORES)})"
        )
    auth = _bloque(fuente, "auth", entry, OPCIONES_AUTH)
    if auth and "env" not in auth:
        raise ValueError(f"Fuente '{fuente}': 'auth' necesita 'env', el NOMBRE de la variable de entorno con la credencial")
    for clave, valor in auth.items():
        if not isinstance(valor, str) or not valor.strip():
            raise ValueError(f"Fuente '{fuente}': auth.{clave} debe ser un texto no vacío, no {valor!r}")
    return DataSource(
        id=entry["id"],
        name=entry["name"],
        url=entry["url"],
        format=fmt,
        options={k: str(v) for k, v in options.items()},
        checks=checks,
        silver=tuple(_silver(fuente, entry.get("silver"))),
        downloader=downloader,
        auth={k: str(v) for k, v in auth.items()},
    )


def _bloque(fuente: str, nombre: str, entry: dict[str, Any], permitidas: set[str]) -> dict[str, Any]:
    bloque = entry.get(nombre) or {}
    if not isinstance(bloque, dict):
        raise ValueError(f"Fuente '{fuente}': '{nombre}' debe ser un mapa clave: valor")
    desconocidas = sorted(set(bloque) - permitidas)
    if desconocidas:
        raise ValueError(
            f"Clave desconocida en {nombre} de la fuente '{fuente}': {', '.join(desconocidas)} (válidas: {', '.join(sorted(permitidas))})"
        )
    return dict(bloque)


def _validar_checks(fuente: str, checks: dict[str, Any]) -> None:
    for clave in ("min_bytes", "min_filas"):
        if clave in checks and (isinstance(checks[clave], bool) or not isinstance(checks[clave], int) or checks[clave] < 1):
            raise ValueError(f"Fuente '{fuente}': checks.{clave} debe ser un entero >= 1, no {checks[clave]!r}")
    if "columnas" in checks:
        columnas = checks["columnas"]
        if not isinstance(columnas, list) or not columnas or not all(isinstance(c, str) and c for c in columnas):
            raise ValueError(f"Fuente '{fuente}': checks.columnas debe ser una lista no vacía de nombres de columna")


def _silver(fuente: str, bruto: Any) -> list[SilverTabla]:
    tablas = []
    for item in bruto or []:
        if not isinstance(item, dict) or set(item) - {"tabla", "modo"}:
            raise ValueError(f"Fuente '{fuente}': cada entrada de 'silver' es {{tabla, modo}}, no {item!r}")
        for clave in ("tabla", "modo"):
            if clave not in item:
                raise ValueError(f"Fuente '{fuente}': a una entrada de 'silver' le falta '{clave}'")
        if not _NOMBRE_TABLA.match(str(item["tabla"])):
            raise ValueError(
                f"Fuente '{fuente}': nombre de tabla Silver inválido '{item['tabla']}' (minúsculas, dígitos y '_', empezando por letra)"
            )
        if item["modo"] not in MODOS_SILVER:
            raise ValueError(f"Fuente '{fuente}': modo Silver no soportado '{item['modo']}' (soportados: {sorted(MODOS_SILVER)})")
        tablas.append(SilverTabla(tabla=item["tabla"], modo=item["modo"]))
    return tablas
