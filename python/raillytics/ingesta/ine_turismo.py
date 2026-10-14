"""Turismo interprovincial del INE: el nivel mensual que a CNMC le falta.

La demanda real del proyecto viene de CNMC, que publica **por trimestre**. El reparto diario de la
predicción tenía por eso un hueco: dentro de un trimestre trataba enero y marzo como el mismo nivel,
porque las reglas de `config/reglas_demanda.yml` solo miraban el día de la semana, los festivos y los
puentes. No había ningún dato con el que saber que marzo mueve más viajes que enero.

Esta tabla lo cubre. El INE estima, mes a mes, cuántos turistas se mueven de una provincia a otra
(operación TMOV, tabla 53267) a partir de la **posición de los teléfonos móviles**: es un conteo, no
una encuesta. La serie empieza en julio de 2019.

**Qué mide y qué no.** Son viajes en todos los medios de transporte, no solo en tren, y «turista» en
TMOV implica pernoctación, así que el viaje de ida y vuelta en el día —mucho del negocio en este
corredor— no entra. Por eso **no se usa su nivel, se usa su forma**: qué peso tiene cada mes dentro
de su trimestre (`raillytics.prediccion.perfil_mensual`). Esa cuota es estable año a año, que es lo
que la hace utilizable; el número absoluto de turistas no es la demanda ferroviaria y no se compara
con ella.

**Dato definitivo o provisional.** El INE marca cada valor con `FK_TipoDato` y los meses recientes
suelen ser provisionales. Se guarda en la columna `tipo_dato` en vez de descartarlos, para que quien
calcule el perfil pueda decidir.

Igual que los festivos: la tabla se construye desde Python y va a Silver con trazabilidad, no se
versiona en el repositorio.

Uso:
    python -m raillytics.ingesta.ine_turismo
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import find_dotenv, load_dotenv

from raillytics.calidad.ficheros import motivo_rechazo, validar_contenido
from raillytics.ingesta import referencia
from raillytics.ingesta.downloaders import obtener
from raillytics.ingesta.sources import load_sources

logger = logging.getLogger(__name__)

PREFIJO_FUENTE = "ine_turismo"
TABLA = "turismo_mensual"

# Tempus3 nombra cada serie «Turistas. <origen>. <destino>. Dato base.». El origen y el destino se
# sacan de ahí y no de la configuración: así la tabla dice lo que el INE dice que descargó, y si el
# INE cambia el formato del nombre esto falla en alto en vez de etiquetar mal las filas.
PREFIJO_SERIE = "Turistas"

# FK_TipoDato del INE. 1 = definitivo, 2 = provisional, 3 = avance.
TIPOS_DE_DATO = {1: "definitivo", 2: "provisional", 3: "avance"}


def _origen_y_destino(nombre: str) -> tuple[str, str]:
    partes = [p.strip() for p in nombre.split(".") if p.strip()]
    if len(partes) < 3 or partes[0] != PREFIJO_SERIE:
        raise ValueError(
            f"la serie '{nombre}' no tiene la forma '{PREFIJO_SERIE}. <origen>. <destino>. ...' "
            "que publica la tabla 53267: revisa si el INE ha cambiado el nombre de las series"
        )
    return partes[1], partes[2]


def parsear(contenido: bytes) -> pd.DataFrame:
    """Convierte la respuesta de Tempus3 en la tabla (fecha, origen, destino, turistas, tipo_dato)."""
    series = json.loads(contenido.decode("utf-8"))
    if not isinstance(series, list) or not series:
        raise ValueError("la respuesta del INE no es una lista de series con datos")

    filas: list[dict] = []
    for serie in series:
        origen, destino = _origen_y_destino(str(serie.get("Nombre", "")))
        for dato in serie.get("Data", []):
            mes = int(dato["FK_Periodo"])
            if not 1 <= mes <= 12:
                # Tempus3 usa el mismo campo para meses, trimestres y semestres. Si llega algo que no
                # es un mes, la tabla pedida no es la mensual y el perfil saldría mal.
                raise ValueError(f"FK_Periodo={mes} no es un mes: la serie '{serie['Nombre']}' no es mensual")
            if dato.get("Secreto"):
                # El INE oculta los valores con riesgo de identificación. Son filas sin dato, no ceros.
                continue
            filas.append({
                # El mes se representa por su primer día: es lo que espera el contrato de Silver,
                # que pide una columna `fecha` de tipo DATE.
                "fecha": date(int(dato["Anyo"]), mes, 1),
                "origen": origen,
                "destino": destino,
                "turistas": float(dato["Valor"]),
                "tipo_dato": TIPOS_DE_DATO.get(int(dato.get("FK_TipoDato", 0)), "desconocido"),
            })

    df = pd.DataFrame(filas, columns=["fecha", "origen", "destino", "turistas", "tipo_dato"])
    if df.empty:
        raise ValueError("el INE no ha devuelto ningún valor utilizable (¿todas las filas son secreto estadístico?)")
    repetidas = df[df.duplicated(["fecha", "origen", "destino"], keep=False)]
    if not repetidas.empty:
        raise ValueError(f"hay meses repetidos para el mismo origen y destino:\n{repetidas}")
    return df.sort_values(["origen", "destino", "fecha"], ignore_index=True)


def construir(registro: Path) -> pd.DataFrame:
    """Descarga las fuentes `ine_turismo*` del registro y devuelve la tabla mensual."""
    fuentes = [f for f in load_sources(registro) if f.id.startswith(PREFIJO_FUENTE)]
    if not fuentes:
        raise ValueError(f"no hay ninguna fuente '{PREFIJO_FUENTE}*' en {registro}")

    trozos: list[pd.DataFrame] = []
    for fuente in sorted(fuentes, key=lambda f: f.id):
        respuesta = obtener(fuente)
        # Los mismos gates que la ingesta: si el INE devuelve una página de error en vez de JSON, no
        # debe acabar en la tabla.
        rechazo = motivo_rechazo(
            validar_contenido(respuesta.content, fuente.format, respuesta.headers.get("Content-Type"),
                              tabla=fuente.id, opciones=fuente.options)
        )
        if rechazo:
            raise ValueError(f"{fuente.id}: la respuesta del INE no pasa los quality gates ({rechazo})")
        de_la_fuente = parsear(respuesta.content)
        logger.info("%s: %d meses", fuente.id, len(de_la_fuente))
        trozos.append(de_la_fuente)

    return pd.concat(trozos, ignore_index=True).sort_values(
        ["origen", "destino", "fecha"], ignore_index=True
    )


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Construye la tabla mensual de turismo interprovincial del INE.")
    p.add_argument("--registro", type=Path, default=Path("config/data_sources.yml"))
    args = p.parse_args(argv)

    df = construir(args.registro)
    destino = referencia.escribir(TABLA, df, {"registro": str(args.registro)}, os.environ, __name__)

    logger.info("%s: %d filas en %s", TABLA, len(df), destino)
    logger.info("  rango: %s .. %s", df["fecha"].min(), df["fecha"].max())
    for tipo, g in df.groupby("tipo_dato"):
        logger.info("  %s: %d meses", tipo, len(g))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
