from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import requests

from raillytics.calidad.ficheros import motivo_rechazo, validar_contenido
from raillytics.calidad.registro import ResultadoGate
from raillytics.ingesta.filenames import staging_filename
from raillytics.ingesta.sources import DataSource
from raillytics.utils.fs import atomic_write_bytes

_TIMEOUT_SECONDS = 30
# Nombre del directorio de cuarentena por defecto, hermano del staging (como
# bronze_l1_done y bronze_processed): nunca lo ve el glob de L1.
REJECTED_DIR_SUFFIX = "_rejected"


@dataclass(frozen=True)
class Descarga:
    """Resultado de una descarga: dónde quedó el fichero y qué dijeron los quality gates."""

    path: Path                    # en el staging si se aceptó; en cuarentena si se rechazó
    bytes: int
    gates: list[ResultadoGate]
    motivo_rechazo: str | None    # None si el fichero pasó todos los gates bloqueantes

    @property
    def aceptada(self) -> bool:
        return self.motivo_rechazo is None


def download(source: DataSource, dest_root: Path, rejected_root: Path | None = None) -> Descarga:
    """Descarga la fuente al staging (dest_root/<source.id>/) si pasa los quality gates de fichero.

    Un fichero rechazado (vacío, o cuyo contenido no es el formato declarado en
    config/data_sources.yml) se guarda en rejected_root/<source.id>/ junto a un
    <nombre>.rechazo.txt con el motivo, para poder inspeccionarlo, y NO entra en
    el pipeline. Quien llama decide si la tarea falla (el DAG lo hace).
    """
    response = requests.get(source.url, timeout=_TIMEOUT_SECONDS)
    response.raise_for_status()
    content = response.content

    gates = validar_contenido(content, source.format, response.headers.get("Content-Type"), tabla=source.id)
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
    return Descarga(path=dest_path, bytes=len(content), gates=gates, motivo_rechazo=motivo)
