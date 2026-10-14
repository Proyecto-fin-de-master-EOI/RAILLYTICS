"""Caché de lo que cuesta de los ZIP del NAP: su vigencia, si es coherente y los trenes por día.

**Por qué existe**, con la medida delante: parsear un snapshot de Renfe tarda 1,45 s y el histórico
tiene 485, así que una pasada completa son unos 12 minutos solo de leer `stop_times.txt`, más 40 s de
vigencias y otros 40 de coherencias. Y ese trabajo se repetía **entero cada día** para añadir un
snapshot nuevo. Con la caché, una pasada diaria son los dos o tres ZIP que han cambiado.

**Por qué se puede cachear sin cambiar el resultado:** los ZIP del NAP son descargas inmutables —el
nombre lleva la fecha y el id de publicación— y las tres cosas que se guardan son **funciones puras
del contenido del fichero**. No se cachea la serie ni la elección de snapshot por día, que sí
dependen de todo el conjunto: se cachea solo el cálculo caro de cada fichero. Por eso la serie que
sale con caché es la misma que sin ella, y hay un test que lo comprueba comparando las dos.

**La clave** incluye el nombre, el tamaño y la fecha de modificación del fichero: un ZIP
re-descargado con otro contenido no acierta. `FORMATO` invalida todo de golpe si cambia el
significado de lo guardado —súbelo si tocas lo que devuelven `vigencia`, `coherente` o
`trenes_por_dias`—.

**Es de mejor esfuerzo**, como la caché del LLM: si el directorio no se puede escribir o una entrada
está corrupta, se calcula y la serie sale igual. La caché nunca hace fallar una carga.
"""
from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Sequence
from pathlib import Path

from raillytics.utils.fs import atomic_write_bytes

logger = logging.getLogger(__name__)

# Súbelo si cambia el significado de lo guardado (invalida toda la caché).
FORMATO = 1

DIRECTORIO_POR_DEFECTO = Path("data/cache/nap_oferta")


class SinCache:
    """La opción de no cachear, para que los sitios que la usan no se llenen de `if cache`."""

    aciertos = 0
    fallos = 0

    def recordar(self, zip_path: Path, etiqueta: str, extra: Sequence[str],
                 calcular: Callable[[], object]) -> object:
        return calcular()

    def resumen(self) -> str:
        return "sin caché"


class CacheSnapshots:
    """Guarda en disco el resultado de un cálculo caro sobre un ZIP, en JSON y uno por entrada.

    `recordar` es la única operación: devuelve lo guardado si la clave acierta y, si no, llama a
    `calcular`, lo guarda y lo devuelve. El valor tiene que ser serializable a JSON; convertirlo es
    cosa de quien llama, para que la caché no tenga que saber nada de fechas ni de corredores.
    """

    def __init__(self, directorio: Path = DIRECTORIO_POR_DEFECTO) -> None:
        self.directorio = directorio
        self.aciertos = 0
        self.fallos = 0

    def _clave(self, zip_path: Path, etiqueta: str, extra: Sequence[str]) -> str:
        try:
            st = zip_path.stat()
            huella: list[object] = [st.st_size, st.st_mtime_ns]
        except OSError:
            # Sin poder mirar el fichero no hay clave fiable: se usa una que no va a acertar.
            huella = ["sin-stat"]
        material = json.dumps([FORMATO, etiqueta, zip_path.name, *huella, list(extra)], sort_keys=True)
        return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]

    def recordar(self, zip_path: Path, etiqueta: str, extra: Sequence[str],
                 calcular: Callable[[], object]) -> object:
        ruta = self.directorio / f"{etiqueta}-{self._clave(zip_path, etiqueta, extra)}.json"
        try:
            # `valor` va envuelto a propósito: así un None cacheado se distingue de un fichero que
            # no está, y `coherente` o un snapshot corrupto se cachean igual que el resto.
            guardado = json.loads(ruta.read_text(encoding="utf-8"))
            if isinstance(guardado, dict) and "valor" in guardado:
                self.aciertos += 1
                return guardado["valor"]
            logger.debug("entrada de caché con formato inesperado, se recalcula: %s", ruta.name)
        except FileNotFoundError:
            pass
        except (OSError, json.JSONDecodeError) as exc:
            logger.debug("entrada de caché ilegible, se recalcula (%s): %s", exc, ruta.name)

        self.fallos += 1
        valor = calcular()
        try:
            self.directorio.mkdir(parents=True, exist_ok=True)
            atomic_write_bytes(ruta, json.dumps({"valor": valor}).encode("utf-8"))
        except (OSError, TypeError, ValueError) as exc:
            # No se puede escribir, o el valor no es serializable: la serie no se entera.
            logger.debug("no se pudo guardar en la caché (%s): %s", exc, ruta.name)
        return valor

    def resumen(self) -> str:
        total = self.aciertos + self.fallos
        if not total:
            return "caché sin usar"
        return f"caché {self.aciertos}/{total} aciertos ({self.aciertos / total:.0%})"
