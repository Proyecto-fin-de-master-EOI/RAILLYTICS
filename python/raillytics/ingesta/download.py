from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

import requests

from raillytics.calidad.ficheros import motivo_rechazo, validar_contenido
from raillytics.calidad.registro import ResultadoGate
from raillytics.ingesta.downloaders import obtener
from raillytics.ingesta.filenames import staging_filename
from raillytics.ingesta.sources import DataSource
from raillytics.utils.fs import atomic_write_bytes

logger = logging.getLogger(__name__)

# Nombre del directorio de cuarentena por defecto, hermano del staging (como
# bronze_l1_done y bronze_processed): nunca lo ve el glob de L1.
REJECTED_DIR_SUFFIX = "_rejected"

# Reintentos ante un fallo TRANSITORIO de la fuente. El plan pide una «tasa de recuperación ante
# fallos de fuente», y sin reintentos esa tasa sería siempre 0: un fallo de red tumbaba la descarga
# del día y no se volvía a intentar hasta la ejecución siguiente.
REINTENTOS = 3
ESPERA_BASE_SEGUNDOS = 2  # 2 s, 4 s, 8 s: suficiente para un corte breve sin colgar el DAG

# Qué se reintenta y qué no, que es la decisión de fondo:
#   SÍ   un corte de red, un timeout o un 5xx: el servidor está caído o saturado y dentro de unos
#        segundos puede responder.
#   NO   un 4xx: la URL está mal, la credencial no vale o el recurso no existe. Reintentar no lo
#        arregla y solo retrasa el fallo. (El 429 de AEMET se trata aparte, dentro de su propio
#        descargador, que sabe leer la cabecera Retry-After.)
#   NO   un fichero RECHAZADO por los quality gates. Si el NAP publica un ZIP que no es un GTFS,
#        volver a bajarlo da el mismo ZIP. Eso no es un fallo transitorio, es la fuente publicando
#        otra cosa, y tiene que verse en rojo.
ERRORES_TRANSITORIOS = (requests.ConnectionError, requests.Timeout)


def _transitorio(exc: Exception) -> bool:
    if isinstance(exc, ERRORES_TRANSITORIOS):
        return True
    respuesta = getattr(exc, "response", None)
    return respuesta is not None and 500 <= respuesta.status_code < 600


@dataclass(frozen=True)
class Descarga:
    """Resultado de una descarga: dónde quedó el fichero y qué dijeron los quality gates."""

    path: Path                    # en el staging si se aceptó; en cuarentena si se rechazó
    bytes: int
    gates: list[ResultadoGate]
    motivo_rechazo: str | None    # None si el fichero pasó todos los gates bloqueantes
    intentos: int = 1             # cuántos hicieron falta; > 1 significa que se recuperó de un fallo

    @property
    def aceptada(self) -> bool:
        return self.motivo_rechazo is None

    @property
    def recuperada(self) -> bool:
        """Hubo un fallo transitorio y el reintento lo salvó. Es el numerador del KPI del plan."""
        return self.intentos > 1


def _traer(source: DataSource, reintentos: int):
    """La respuesta de la fuente y cuántos intentos costó. Reintenta solo lo transitorio."""
    for intento in range(1, reintentos + 1):
        try:
            return obtener(source), intento
        except Exception as exc:  # noqa: BLE001 - se re-lanza lo que no sea transitorio
            if intento == reintentos or not _transitorio(exc):
                raise
            espera = ESPERA_BASE_SEGUNDOS * 2 ** (intento - 1)
            logger.warning("  %s: fallo transitorio (%s). Se reintenta en %ds (%d/%d)",
                           source.id, type(exc).__name__, espera, intento, reintentos)
            time.sleep(espera)
    raise AssertionError("inalcanzable")  # pragma: no cover


def download(source: DataSource, dest_root: Path, rejected_root: Path | None = None,
             reintentos: int = REINTENTOS) -> Descarga:
    """Descarga la fuente al staging (dest_root/<source.id>/) si pasa los quality gates de fichero.

    Un fichero rechazado (vacío, o cuyo contenido no es el formato declarado en
    config/data_sources.yml) se guarda en rejected_root/<source.id>/ junto a un
    <nombre>.rechazo.txt con el motivo, para poder inspeccionarlo, y NO entra en
    el pipeline. Quien llama decide si la tarea falla (el DAG lo hace).
    """
    # Cómo se trae la fuente lo decide su `downloader` (ver downloaders.py): `http`
    # es un GET de `url`, y AEMET y el NAP aportan el suyo porque su descarga es un
    # procedimiento de varios pasos. Lo de aquí abajo no cambia según la fuente.
    response, intentos = _traer(source, reintentos)
    content = response.content

    gates = validar_contenido(
        content, source.format, response.headers.get("Content-Type"), tabla=source.id, opciones=source.options, checks=source.checks
    )
    motivo = motivo_rechazo(gates)
    file_name = staging_filename(source, response.headers)

    if motivo is None:
        dest_dir = dest_root / source.id
    else:
        dest_dir = (rejected_root or dest_root.with_name(dest_root.name + REJECTED_DIR_SUFFIX)) / source.id
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = atomic_write_bytes(dest_dir / file_name, content)
    if motivo is not None:
        (dest_dir / f"{file_name}.rechazo.txt").write_text(motivo + "\n", encoding="utf-8")
    return Descarga(path=dest_path, bytes=len(content), gates=gates, motivo_rechazo=motivo,
                    intentos=intentos)
