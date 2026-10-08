"""Quality Gates sobre el fichero descargado (entrada de Bronze).

Es el único control que no puede hacer Spark: se aplica en la descarga Python,
antes de que el fichero llegue al staging que vigila L1. Un fichero que no pasa
un gate bloqueante no entra en el pipeline (download.py lo deja en cuarentena).

Gates (todos sobre el contenido en memoria, sin tocar disco):

  contenido_no_vacio   bloqueante  el servidor devolvió bytes
  formato_declarado    bloqueante  el contenido ES lo que dice config/data_sources.yml:
                                   csv -> texto con cabecera delimitada; json -> JSON válido;
                                   xml -> XML bien formado y con contenido; zip -> archivo ZIP
                                   íntegro con al menos un miembro
  content_type         aviso       la cabecera Content-Type del servidor es coherente con el formato
  tamano_minimo        bloqueante  (solo si la fuente declara checks.min_bytes) el fichero pesa al menos eso
  cabecera_esperada    bloqueante  (checks.columnas) la cabecera, sin BOM y partida con el delimitador, es la esperada
  filas_minimas        bloqueante  (checks.min_filas) hay al menos esas filas de datos

Motivación: CRTM sirve un ZIP (GTFS) y Renfe JSON; con el formato mal declarado L2
convertía bytes binarios a Parquet sin que nada avisara.
"""
from __future__ import annotations

import csv
import io
import json
import zipfile
from collections.abc import Mapping, Sequence

# defusedxml y no xml.etree directamente: el XML llega de internet y el parser de la librería
# estándar es vulnerable a la expansión de entidades (el «billion laughs»).
from defusedxml.ElementTree import ParseError, fromstring as _parsear_xml

from raillytics.calidad.registro import (
    RESULTADO_FALLO,
    RESULTADO_OK,
    SEVERIDAD_AVISO,
    SEVERIDAD_BLOQUEANTE,
    ResultadoGate,
)

TIPO_FICHERO = "fichero"

# Firma de un ZIP: cabecera del primer miembro, o el fin de directorio central
# en un archivo sin miembros (que también es un ZIP válido, aunque inútil).
_ZIP_MAGICS = (b"PK\x03\x04", b"PK\x05\x06")
_CSV_DELIMITERS = (",", ";", "\t", "|")
# Content-Type que se consideran coherentes con cada formato (sin parámetros como charset).
_CONTENT_TYPES = {
    "csv": {"text/csv", "text/plain", "application/csv", "application/vnd.ms-excel"},
    "json": {"application/json", "text/json", "application/x-json"},
    "xml": {"application/xml", "text/xml"},
    "zip": {"application/zip", "application/x-zip-compressed", "application/octet-stream"},
}
# Lo que delata que el contenido no es ni texto tabular ni JSON: marcado o binario.
_NI_TEXTO_NI_JSON = ("ZIP", "binario", "HTML", "XML", "HTML/XML")


def validar_contenido(
    content: bytes,
    formato: str,
    content_type: str | None = None,
    tabla: str = "",
    opciones: Mapping[str, str] | None = None,
    checks: Mapping[str, object] | None = None,
) -> list[ResultadoGate]:
    """Evalúa los gates de fichero y devuelve un resultado por gate (ninguno lanza).

    `opciones` (delimiter, encoding) y `checks` (min_bytes, min_filas, columnas) salen de la fuente en
    config/data_sources.yml. Los checks solo se evalúan si el formato declarado ha pasado: si el servidor
    devolvió una página HTML, el único motivo del rechazo debe ser ese.
    """
    opciones = opciones or {}
    resultados = [_gate_no_vacio(content, tabla)]
    formato_ok = False
    if resultados[0].pasa:
        gate_formato = _gate_formato(content, formato, tabla, opciones)
        resultados.append(gate_formato)
        formato_ok = gate_formato.pasa
    resultados.append(_gate_content_type(content_type, formato, tabla))
    if formato_ok and checks:
        resultados.extend(_gates_declarados(content, tabla, opciones, checks))
    return resultados


def motivo_rechazo(resultados: Sequence[ResultadoGate]) -> str | None:
    """Texto con los gates bloqueantes fallidos, o None si el fichero es aceptable."""
    fallidos = [r for r in resultados if r.bloquea]
    if not fallidos:
        return None
    return "; ".join(f"{r.gate}: {r.detalle}" for r in fallidos)


def _resultado(tabla: str, gate: str, severidad: str, ok: bool, detalle: str | None, valor: float | None = None,
               umbral: str = "= 1") -> ResultadoGate:
    return ResultadoGate(
        tabla=tabla, gate=gate, tipo=TIPO_FICHERO, severidad=severidad,
        resultado=RESULTADO_OK if ok else RESULTADO_FALLO,
        valor=valor if valor is not None else (1.0 if ok else 0.0), umbral=umbral, detalle=detalle,
    )


def _gate_no_vacio(content: bytes, tabla: str) -> ResultadoGate:
    return _resultado(
        tabla, "contenido_no_vacio", SEVERIDAD_BLOQUEANTE, len(content) > 0,
        None if content else "el servidor devolvió 0 bytes", valor=float(len(content)), umbral="> 0",
    )


def _gate_formato(content: bytes, formato: str, tabla: str, opciones: Mapping[str, str]) -> ResultadoGate:
    problema = _comprobar_formato(content, formato, opciones)
    return _resultado(tabla, "formato_declarado", SEVERIDAD_BLOQUEANTE, problema is None, problema)


def _gate_content_type(content_type: str | None, formato: str, tabla: str) -> ResultadoGate:
    if not content_type:
        return _resultado(tabla, "content_type", SEVERIDAD_AVISO, True, "sin cabecera Content-Type")
    tipo = content_type.split(";")[0].strip().lower()
    coherente = tipo in _CONTENT_TYPES.get(formato, set())
    return _resultado(
        tabla, "content_type", SEVERIDAD_AVISO, coherente,
        None if coherente else f"Content-Type '{tipo}' no es el esperado para formato '{formato}'",
    )


def _describir_contenido(content: bytes, codificacion: str = "utf-8") -> str:
    """Qué parece ser el contenido, para el mensaje de error. `codificacion` es la declarada por la fuente (si la hay)."""
    if content.startswith(_ZIP_MAGICS):
        return "ZIP"
    inicio = content[:512].lstrip()
    if inicio[:1] in (b"{", b"["):
        return "JSON"
    if inicio[:1] == b"<":
        # Distinguir HTML de XML importa: una página de error de un proxy («<html><body>...») es
        # XML bien formado, así que sin esto colaría como xml válido. Cuando no hay forma de
        # saberlo (un `<foo>` a secas) se devuelve el genérico y el formato declarado decide.
        cabeza = inicio[:512].lower()
        if cabeza.startswith(b"<?xml"):
            return "XML"
        if cabeza.startswith((b"<!doctype html", b"<html")):
            return "HTML"
        return "HTML/XML"
    try:
        content[:4096].decode(codificacion)
    except UnicodeDecodeError:
        return "binario"
    return "texto"


def _comprobar_formato(content: bytes, formato: str, opciones: Mapping[str, str] | None = None) -> str | None:
    """None si el contenido encaja con el formato declarado; si no, el motivo."""
    parece = _describir_contenido(content, (opciones or {}).get("encoding", "utf-8"))
    if formato == "zip":
        if parece != "ZIP":
            return f"declarado zip, el contenido parece {parece}"
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                miembros = [m for m in zf.namelist() if not m.endswith("/")]
                if not miembros:
                    return "el ZIP no contiene ningún fichero"
                corrupto = zf.testzip()
                if corrupto is not None:
                    return f"el ZIP tiene un miembro corrupto: {corrupto}"
        except zipfile.BadZipFile as exc:
            return f"ZIP inválido: {exc}"
        return None
    if formato == "xml":
        if parece == "HTML":
            return "declarado xml, el contenido parece HTML (¿una página de error del servidor?)"
        if parece not in ("XML", "HTML/XML"):
            return f"declarado xml, el contenido parece {parece}"
        try:
            raiz = _parsear_xml(content)
        except (ParseError, ValueError) as exc:  # ValueError cubre las defensas de defusedxml
            return f"XML inválido: {exc}"
        if len(raiz) == 0 and not (raiz.text or "").strip():
            return f"el XML solo trae el elemento raíz <{raiz.tag}> vacío"
        return None
    if formato == "json":
        if parece in _NI_TEXTO_NI_JSON:
            return f"declarado json, el contenido parece {parece}"
        try:
            json.loads(content)
        except ValueError as exc:
            return f"JSON inválido: {exc}"
        return None
    if formato == "csv":
        if parece in _NI_TEXTO_NI_JSON + ("JSON",):
            return f"declarado csv, el contenido parece {parece}"
        opciones = opciones or {}
        # Con delimitador declarado, ese y solo ese: declarar `,` sobre un fichero con `;` es un error de configuración.
        delimitadores = (opciones["delimiter"],) if "delimiter" in opciones else _CSV_DELIMITERS
        cabecera = content.split(b"\n", 1)[0].decode(opciones.get("encoding", "utf-8"), errors="replace").strip().lstrip("\ufeff")
        if not cabecera:
            return "la primera línea (cabecera) está vacía"
        if not any(d in cabecera for d in delimitadores):
            return f"la cabecera no tiene delimitador: {cabecera[:80]!r}"
        return None
    return f"formato '{formato}' sin validador"


def _gates_declarados(content: bytes, tabla: str, opciones: Mapping[str, str], checks: Mapping[str, object]) -> list[ResultadoGate]:
    """Gates que la fuente pide con `checks` (todos bloqueantes: un fichero que no los cumple no entra en el pipeline)."""
    resultados: list[ResultadoGate] = []
    if "min_bytes" in checks:
        minimo = int(checks["min_bytes"])
        resultados.append(
            _resultado(
                tabla, "tamano_minimo", SEVERIDAD_BLOQUEANTE, len(content) >= minimo,
                f"el fichero pesa {len(content)} bytes, se esperaban al menos {minimo}", valor=float(len(content)), umbral=f">= {minimo}",
            )
        )
    texto = content.decode(opciones.get("encoding", "utf-8"), errors="replace").lstrip("\ufeff")
    delimitador = opciones.get("delimiter", ",")
    lineas = texto.splitlines()
    if "columnas" in checks:
        esperadas = [str(c) for c in checks["columnas"]]
        cabecera = next(csv.reader(lineas[:1], delimiter=delimitador), []) if lineas else []
        ok = [c.strip().lower() for c in cabecera] == [c.strip().lower() for c in esperadas]
        resultados.append(
            _resultado(
                tabla, "cabecera_esperada", SEVERIDAD_BLOQUEANTE, ok,
                f"cabecera {cabecera} distinta de la esperada {esperadas}", valor=float(len(cabecera)), umbral=f"= {len(esperadas)} columnas",
            )
        )
    if "min_filas" in checks:
        minimo = int(checks["min_filas"])
        filas = sum(1 for linea in lineas[1:] if linea.strip())
        resultados.append(
            _resultado(
                tabla, "filas_minimas", SEVERIDAD_BLOQUEANTE, filas >= minimo,
                f"{filas} fila(s) de datos, se esperaban al menos {minimo}", valor=float(filas), umbral=f">= {minimo}",
            )
        )
    return resultados
