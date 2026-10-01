"""Quality Gates sobre el fichero descargado (entrada de Bronze).

Es el único control que no puede hacer Spark: se aplica en la descarga Python,
antes de que el fichero llegue al staging que vigila L1. Un fichero que no pasa
un gate bloqueante no entra en el pipeline (download.py lo deja en cuarentena).

Gates (todos sobre el contenido en memoria, sin tocar disco):

  contenido_no_vacio   bloqueante  el servidor devolvió bytes
  formato_declarado    bloqueante  el contenido ES lo que dice config/data_sources.yml:
                                   csv -> texto con cabecera delimitada; json -> JSON válido;
                                   zip -> archivo ZIP íntegro con al menos un miembro
  content_type         aviso       la cabecera Content-Type del servidor es coherente con el formato

Motivación: CRTM sirve un ZIP (GTFS) y Renfe JSON; con el formato mal declarado L2
convertía bytes binarios a Parquet sin que nada avisara.
"""
from __future__ import annotations

import io
import json
import zipfile
from collections.abc import Sequence

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
    "zip": {"application/zip", "application/x-zip-compressed", "application/octet-stream"},
}


def validar_contenido(content: bytes, formato: str, content_type: str | None = None, tabla: str = "") -> list[ResultadoGate]:
    """Evalúa los gates de fichero y devuelve un resultado por gate (ninguno lanza)."""
    resultados = [_gate_no_vacio(content, tabla)]
    if resultados[0].pasa:
        resultados.append(_gate_formato(content, formato, tabla))
    resultados.append(_gate_content_type(content_type, formato, tabla))
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


def _gate_formato(content: bytes, formato: str, tabla: str) -> ResultadoGate:
    problema = _comprobar_formato(content, formato)
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


def _describir_contenido(content: bytes) -> str:
    """Qué parece ser el contenido, para el mensaje de error."""
    if content.startswith(_ZIP_MAGICS):
        return "ZIP"
    inicio = content[:512].lstrip()
    if inicio[:1] in (b"{", b"["):
        return "JSON"
    if inicio[:1] == b"<":
        return "HTML/XML"
    try:
        content[:4096].decode("utf-8")
    except UnicodeDecodeError:
        return "binario"
    return "texto"


def _comprobar_formato(content: bytes, formato: str) -> str | None:
    """None si el contenido encaja con el formato declarado; si no, el motivo."""
    parece = _describir_contenido(content)
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
    if formato == "json":
        if parece in ("ZIP", "binario", "HTML/XML"):
            return f"declarado json, el contenido parece {parece}"
        try:
            json.loads(content)
        except ValueError as exc:
            return f"JSON inválido: {exc}"
        return None
    if formato == "csv":
        if parece in ("ZIP", "binario", "HTML/XML", "JSON"):
            return f"declarado csv, el contenido parece {parece}"
        cabecera = content.split(b"\n", 1)[0].decode("utf-8", errors="replace").strip()
        if not cabecera:
            return "la primera línea (cabecera) está vacía"
        if not any(d in cabecera for d in _CSV_DELIMITERS):
            return f"la cabecera no tiene delimitador: {cabecera[:80]!r}"
        return None
    return f"formato '{formato}' sin validador"
