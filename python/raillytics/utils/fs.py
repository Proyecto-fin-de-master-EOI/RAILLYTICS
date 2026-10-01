from __future__ import annotations

from pathlib import Path


def atomic_write_bytes(dest_path: Path, content: bytes) -> Path:
    # Escritura a fichero temporal + rename atómico: si la escritura se corta a
    # medias, Spark nunca llega a ver un fichero incompleto en staging.
    # El prefijo "." (no solo el sufijo ".tmp") es importante: el listado de
    # ficheros de streaming de Spark solo filtra nombres que empiezan por "."
    # o "_", no sufijos, así que un fichero a medias con solo sufijo ".tmp"
    # podría llegar a ingerirse como si fuera un dato real.
    tmp_path = dest_path.parent / f".{dest_path.name}.tmp"
    tmp_path.write_bytes(content)
    tmp_path.rename(dest_path)
    return dest_path
