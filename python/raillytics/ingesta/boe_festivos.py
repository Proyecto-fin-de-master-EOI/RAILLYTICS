"""Festivos del corredor a partir del calendario laboral del BOE.

Cada año el BOE publica una resolución con la tabla de fiestas laborales: una fila por festivo y
una columna por comunidad autónoma, marcada con asteriscos. Esto la convierte en la tabla
(fecha, nombre) que consume el origen `festivos` de la predicción.

**Qué cuenta como festivo aquí:** los días festivos en **Madrid o en Cataluña**. El corredor une
las dos ciudades, así que un festivo en cualquiera de los dos extremos mueve viajes, aunque no sea
nacional (la Diada en Cataluña, el 2 de mayo en Madrid). El contrato de la predicción admite una
sola fila por fecha, así que el ámbito se mete en el nombre: es lo que acaba viendo el LLM en el
calendario, y le permite distinguir un festivo nacional de uno de un solo extremo.

**Lo que NO está en esta tabla:** las fiestas locales de cada municipio (dos al año), que no las
publica el BOE sino cada comunidad autónoma. San Isidro en Madrid o la Mercè en Barcelona son
locales, así que no salen aquí.

Las URL no se repiten: salen de las fuentes `boe_*` de config/data_sources.yml, y la descarga pasa
por el mismo descargador y los mismos quality gates que la ingesta, para que el dato tenga la misma
trazabilidad que el resto.

Uso:
    python -m raillytics.ingesta.boe_festivos
"""
from __future__ import annotations

import argparse
import logging
import os
import re
from datetime import date
from pathlib import Path

import pandas as pd
from defusedxml.ElementTree import fromstring
from dotenv import find_dotenv, load_dotenv

from raillytics.calidad.ficheros import motivo_rechazo, validar_contenido
from raillytics.ingesta import referencia
from raillytics.ingesta.downloaders import obtener
from raillytics.ingesta.sources import DataSource, load_sources

logger = logging.getLogger(__name__)

PREFIJO_FUENTE = "boe_calendario_laboral_"

MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

# Las dos comunidades del corredor, como aparecen en la cabecera de la tabla. Se buscan por
# prefijo porque el BOE les añade llamadas a pie de tabla («Cataluña (2)»).
COMUNIDADES = {"Madrid": "Com. Madrid", "Cataluña": "Cataluña"}

# En la tabla, `*` y `**` son fiesta nacional (no sustituible / sin ejercer la sustitución) y `***`
# es de la comunidad autónoma. Da igual cuál sea: lo que importa es si la celda de la comunidad
# tiene marca, porque eso es lo que dice si ese día se trabaja allí o no.

# «1 Año Nuevo.» -> día y nombre. El punto final se quita porque es del documento, no del nombre.
_FILA_FESTIVO = re.compile(r"^(\d{1,2})\s+(.*?)\.?$")


def _celdas(tr) -> list[str]:
    return ["".join(td.itertext()).strip() for td in tr]


def _columnas_del_corredor(cabecera: list[str]) -> dict[str, int]:
    """Índice de celda de cada comunidad del corredor, localizada por su nombre en la cabecera.

    Se busca en vez de fijar la posición porque el orden o las llamadas de la tabla pueden cambiar
    de un año a otro. Si alguna no aparece, es mejor fallar que contar festivos de otra comunidad.
    """
    indices = {}
    for etiqueta, nombre in COMUNIDADES.items():
        encontrados = [i for i, c in enumerate(cabecera) if c.startswith(nombre)]
        if len(encontrados) != 1:
            raise ValueError(
                f"'{nombre}' aparece {len(encontrados)} veces en la cabecera de la tabla del BOE: {cabecera}"
            )
        # La primera celda de cada fila es la fecha, así que las comunidades van desplazadas una.
        indices[etiqueta] = encontrados[0] + 1
    return indices


def parsear(xml: bytes, anio: int) -> list[dict]:
    """Festivos de Madrid o Cataluña de la resolución del BOE, como filas (fecha, nombre)."""
    raiz = fromstring(xml)
    titulo = raiz.findtext(".//titulo") or ""
    # Red de seguridad contra apuntar una fuente al BOE de otro año: el título lo dice.
    if str(anio) not in titulo:
        raise ValueError(f"la resolución del BOE no menciona el año {anio}: {titulo[:120]!r}")

    tabla = raiz.find(".//table")
    if tabla is None:
        raise ValueError("la resolución del BOE no trae la tabla de festivos")
    filas = [_celdas(tr) for tr in tabla.iter("tr")]

    # La cabecera de comunidades es la primera fila con más de dos celdas; la anterior es el
    # título de la tabla («Fecha de las fiestas | Comunidades Autónomas»).
    cabecera = next((f for f in filas if len(f) > 2), None)
    if cabecera is None:
        raise ValueError("no se encuentra la cabecera de comunidades en la tabla del BOE")
    columnas = _columnas_del_corredor(cabecera)

    festivos: list[dict] = []
    mes = None
    for fila in filas[filas.index(cabecera) + 1:]:
        primera = fila[0]
        # Fila de mes: solo trae el nombre del mes y el resto de celdas vacías.
        if primera.lower() in MESES and not any(c for c in fila[1:]):
            mes = MESES[primera.lower()]
            continue
        casa = _FILA_FESTIVO.match(primera)
        if not casa or mes is None:
            continue
        dia, nombre = int(casa.group(1)), casa.group(2).strip()
        conmarca = {e for e, i in columnas.items() if i < len(fila) and fila[i]}
        if not conmarca:
            continue  # festivo de otras comunidades: no afecta al corredor
        festivos.append({"fecha": date(anio, mes, dia), "nombre": _nombrar(nombre, conmarca)})
    return festivos


def _nombrar(nombre: str, comunidades: set[str]) -> str:
    """Nombre del festivo, con el extremo del corredor cuando solo lo es en uno de los dos.

    El nombre es lo que la predicción le pasa al LLM en la línea del calendario, así que decir
    «(Madrid)» le permite valorar distinto un día en el que solo se para un extremo.

    El ámbito se decide por las dos comunidades del corredor, NO por el tipo de marca del BOE: el
    Jueves Santo es fiesta nacional y lleva marca de nacional, pero en Cataluña no es festivo, así
    que para este corredor es un festivo de un solo extremo.
    """
    if comunidades == set(COMUNIDADES):
        return nombre
    return f"{nombre} ({' y '.join(sorted(comunidades))})"


def _anio_de(fuente: DataSource) -> int:
    sufijo = fuente.id[len(PREFIJO_FUENTE):]
    if not sufijo.isdigit():
        raise ValueError(f"la fuente '{fuente.id}' no acaba en un año: no se sabe de qué calendario es")
    return int(sufijo)


def construir(registro: Path) -> pd.DataFrame:
    """Descarga las fuentes `boe_*` del registro y devuelve la tabla (fecha, nombre)."""
    fuentes = [f for f in load_sources(registro) if f.id.startswith(PREFIJO_FUENTE)]
    if not fuentes:
        raise ValueError(f"no hay ninguna fuente '{PREFIJO_FUENTE}*' en {registro}")

    filas: list[dict] = []
    for fuente in sorted(fuentes, key=lambda f: f.id):
        anio = _anio_de(fuente)
        respuesta = obtener(fuente)
        # Los mismos gates que la ingesta: un BOE que no es XML no debe acabar en la tabla.
        rechazo = motivo_rechazo(
            validar_contenido(respuesta.content, fuente.format, respuesta.headers.get("Content-Type"),
                              tabla=fuente.id, opciones=fuente.options)
        )
        if rechazo:
            raise ValueError(f"{fuente.id}: el BOE descargado no pasa los quality gates ({rechazo})")
        del_anio = parsear(respuesta.content, anio)
        logger.info("%s: %d festivos de Madrid o Cataluña", fuente.id, len(del_anio))
        filas.extend(del_anio)

    df = pd.DataFrame(filas, columns=["fecha", "nombre"])
    # El contrato de la predicción exige una fila por fecha. Dos años distintos no pueden chocar,
    # pero si una fuente se duplicara en el registro saldrían fechas repetidas.
    repetidas = df[df.duplicated("fecha", keep=False)]
    if not repetidas.empty:
        raise ValueError(f"hay fechas repetidas en el calendario del BOE:\n{repetidas}")
    return df.sort_values("fecha", ignore_index=True)


def main(argv: list[str] | None = None) -> int:
    # Las rutas del lago salen del .env, igual que en el resto del pipeline.
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Construye la tabla de festivos del corredor desde el BOE.")
    p.add_argument("--registro", type=Path, default=Path("config/data_sources.yml"))
    args = p.parse_args(argv)

    df = construir(args.registro)
    destino = referencia.escribir(
        "festivos", df, {"registro": str(args.registro)}, os.environ, __name__
    )

    logger.info("festivos: %d filas en %s", len(df), destino)
    logger.info("  rango: %s .. %s", df["fecha"].min(), df["fecha"].max())
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
