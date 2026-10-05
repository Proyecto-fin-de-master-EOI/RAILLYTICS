"""Serie diaria de oferta: cuántos trenes pone cada operador cada día en cada corredor.

Se construye a partir del histórico de GTFS que baja `nap_historico`. Es la variable DIARIA que la
demanda de CNMC no tiene (ella es trimestral), así que es lo que permite comprobar si el reparto
diario de la DTC se parece a la oferta real, en vez de solo cuadrar el total del trimestre.

Tres cosas que no son obvias y que, sin ellas, la serie sale mal en silencio:

1. **El snapshot del día X no describe el día X.** Renfe publica casi siempre con vigencia
   inmediata, pero OUIGO a veces publica el horario de la temporada siguiente: hay ficheros de
   septiembre cuyo calendario empieza en diciembre. Para cada fecha se elige el snapshot más
   reciente publicado en o antes de esa fecha **cuyo calendario la cubra**; si ninguno la cubre, el
   día queda sin dato en vez de a cero.
2. **Hay snapshots publicados corruptos**: `stop_times.txt` y `trips.txt` traen esquemas de
   `trip_id` distintos y no casan en ninguno. El conteo saldría 0 sin dar error, que en un
   dashboard se lee como «no circuló ningún tren». Se detectan y se recurre al snapshot anterior.
3. **Los trips del GTFS están duplicados por patrón de calendario**, así que contarlos
   directamente infla la cifra: hay que expandir calendar.txt y calendar_dates.txt.

Uso:
    python -m raillytics.ingesta.nap_oferta
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import zipfile
from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import find_dotenv, load_dotenv

from raillytics.ingesta import referencia

logger = logging.getLogger(__name__)

# Estaciones cabecera de cada corredor (stop_id del GTFS de Renfe, estables entre 2017 y 2026).
# Un trip cuenta para el corredor si para en alguna estación de Madrid y en alguna del destino.
MADRID = {"60000", "17000"}  # Puerta de Atocha, Chamartín
CORREDORES = {
    "Madrid-Barcelona": {"71801"},                # Barcelona-Sants: el corredor del proyecto
    "Madrid-Valencia": {"65000", "03216"},        # Nord, Joaquín Sorolla
    "Madrid-Alicante": {"60911"},                 # Alicante/Alacant-Terminal
    "Madrid-Sevilla": {"51003"},                  # Sevilla-Santa Justa
    "Madrid-Málaga/Granada": {"54413", "05000"},  # María Zambrano, Granada
}

DIAS_SEMANA = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

# Productos de alta velocidad, para poder comparar con la demanda de CNMC: en estos corredores
# CNMC solo publica «LD AV». El GTFS de Renfe mezcla productos (aparecen ALVIA, Intercity,
# REG.EXP.), así que se filtra por `route_short_name`.
#
# ALVIA queda fuera a propósito: circula en parte por vía convencional y CNMC lo clasifica aparte
# de «LD AV». Es la decisión discutible del filtro, así que cada registro guarda también
# `trenes_todos` sin filtrar, para poder rehacer la cuenta con otro criterio.
PRODUCTOS_ALTA_VELOCIDAD = {"AVE", "AVE INT", "AVLO", "TGV"}

# Días a cada lado que se miran para decidir si un 0 es real o un artefacto de cambio de horario.
VENTANA_CEROS = 3

# Ficheros del GTFS sin los que no se puede contar nada.
GTFS_MINIMO = {"stop_times.txt", "trips.txt", "calendar.txt", "routes.txt"}

# Fallos esperables al abrir un ZIP que viene de internet: no deben tirar un proceso de minutos.
ERRORES_LECTURA = (OSError, zipfile.BadZipFile, KeyError, csv.Error, UnicodeDecodeError)


def _norm_stop(stop_id: str) -> str:
    """Normaliza un stop_id para poder comparar entre operadores.

    Renfe usa "60000"; OUIGO usa "007160000", el mismo código con el prefijo UIC de España. Sin
    normalizar, la cuenta de OUIGO saldría 0.
    """
    s = stop_id.strip()
    if len(s) > 5 and s.startswith("0071"):
        s = s[4:]
    return s.lstrip("0") or "0"


def _leer_csv(zf: zipfile.ZipFile, nombre: str) -> list[dict]:
    """Lee un CSV del GTFS limpiando el relleno de espacios que mete el NAP."""
    with zf.open(nombre) as f:
        lector = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        return [{(k or "").strip(): (v or "").strip() for k, v in fila.items()} for fila in lector]


def vigencia(zip_path: Path) -> tuple[str, str] | None:
    """Rango de fechas (AAAAMMDD) que cubre un GTFS, leyendo solo calendar.txt.

    Barato a propósito: no abre stop_times.txt, que es el fichero grande.
    """
    try:
        with zipfile.ZipFile(zip_path) as zf:
            if "calendar.txt" not in zf.namelist():
                return None
            cal = _leer_csv(zf, "calendar.txt")
        if not cal:
            return None
        return min(c["start_date"] for c in cal), max(c["end_date"] for c in cal)
    except ERRORES_LECTURA as exc:
        logger.warning("  %s: no se puede leer la vigencia (%s)", zip_path.stem, exc)
        return None


def coherente(zip_path: Path) -> bool:
    """¿Casan los trip_id de stop_times.txt con los de trips.txt?

    Comprobación barata, sobre una muestra: no recorre stop_times.txt entero.
    """
    try:
        with zipfile.ZipFile(zip_path) as zf:
            if not {"stop_times.txt", "trips.txt"} <= set(zf.namelist()):
                return False
            ids_trips = {t["trip_id"] for t in _leer_csv(zf, "trips.txt")}
            if not ids_trips:
                return False
            with zf.open("stop_times.txt") as f:
                lector = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                muestra = {(fila.get("trip_id") or "").strip() for fila, _ in zip(lector, range(200))}
        return bool(muestra & ids_trips)
    except ERRORES_LECTURA:
        return False


def trenes_por_dias(zip_path: Path, dias: Sequence[date]) -> dict[date, dict[str, dict[str, int]]] | None:
    """Trenes por corredor de cada día pedido, o None si el snapshot está corrupto.

    Abre y parsea el ZIP **una sola vez** para todos los días: un snapshot de OUIGO cubre varias
    semanas, y reparsear stop_times.txt por cada día multiplicaría el trabajo sin cambiar nada.
    """
    with zipfile.ZipFile(zip_path) as zf:
        nombres = set(zf.namelist())
        if not GTFS_MINIMO <= nombres:
            return None
        stop_times = _leer_csv(zf, "stop_times.txt")
        trips = _leer_csv(zf, "trips.txt")
        rutas = _leer_csv(zf, "routes.txt")
        calendario = _leer_csv(zf, "calendar.txt")
        excepciones = _leer_csv(zf, "calendar_dates.txt") if "calendar_dates.txt" in nombres else []

    paradas_por_trip: dict[str, set[str]] = defaultdict(set)
    for fila in stop_times:
        paradas_por_trip[fila["trip_id"]].add(_norm_stop(fila["stop_id"]))

    servicio_por_trip = {t["trip_id"]: t["service_id"] for t in trips}
    # Snapshot corrupto: los dos ficheros usan esquemas de trip_id que no casan en ninguno. Sin
    # esto el conteo sale 0 en silencio y se leería como que no circuló ningún tren.
    if paradas_por_trip and not (set(paradas_por_trip) & set(servicio_por_trip)):
        return None

    producto_por_ruta = {r["route_id"]: r.get("route_short_name", "") for r in rutas}
    producto_por_trip = {t["trip_id"]: producto_por_ruta.get(t.get("route_id", ""), "") for t in trips}
    cal = {c["service_id"]: c for c in calendario}
    exc: dict[str, dict[str, str]] = defaultdict(dict)
    for e in excepciones:
        exc[e["service_id"]][e["date"]] = e["exception_type"]

    madrid_norm = {_norm_stop(s) for s in MADRID}
    # Qué trips tocan cada corredor no depende del día: se resuelve una vez para todos.
    trips_por_corredor = {
        corredor: [
            trip_id
            for trip_id, paradas in paradas_por_trip.items()
            if paradas & madrid_norm and paradas & {_norm_stop(s) for s in destinos}
        ]
        for corredor, destinos in CORREDORES.items()
    }

    resultado: dict[date, dict[str, dict[str, int]]] = {}
    for dia in dias:
        dia_str = dia.strftime("%Y%m%d")
        nombre_dia = DIAS_SEMANA[dia.weekday()]

        def circula(service_id: str) -> bool:
            # calendar_dates.txt manda sobre calendar.txt: es la excepción a la regla semanal.
            if service_id in exc and dia_str in exc[service_id]:
                return exc[service_id][dia_str] == "1"
            c = cal.get(service_id)
            if not c or not (c["start_date"] <= dia_str <= c["end_date"]):
                return False
            return c.get(nombre_dia) == "1"

        del_dia = {}
        for corredor, trip_ids in trips_por_corredor.items():
            av = todos = 0
            for trip_id in trip_ids:
                if not circula(servicio_por_trip.get(trip_id, "")):
                    continue
                todos += 1
                if producto_por_trip.get(trip_id, "") in PRODUCTOS_ALTA_VELOCIDAD:
                    av += 1
            del_dia[corredor] = {"alta_velocidad": av, "todos": todos}
        resultado[dia] = del_dia
    return resultado


def elegir_snapshots(zips: Iterable[Path], desde: date, hasta: date) -> dict[date, Path]:
    """Para cada fecha, el snapshot vigente más reciente que no esté corrupto.

    El nombre del fichero es `AAAA-MM-DD_<id>.zip`, así que la fecha de publicación sale del nombre
    sin necesidad de abrirlo.
    """
    vigencias = []  # (publicacion AAAAMMDD, inicio, fin, path)
    for z in sorted(zips):
        v = vigencia(z)
        if v:
            vigencias.append((z.stem.split("_")[0].replace("-", ""), v[0], v[1], z))
    vigencias.sort()

    # El veredicto de validez es propiedad del fichero, no de la fecha: se comprueba una vez.
    veredicto: dict[Path, bool] = {}

    def valido(path: Path) -> bool:
        if path not in veredicto:
            veredicto[path] = coherente(path)
            if not veredicto[path]:
                logger.warning("  snapshot corrupto, se descarta: %s", path.stem)
        return veredicto[path]

    elegido: dict[date, Path] = {}
    dia = desde
    while dia <= hasta:
        ds = dia.strftime("%Y%m%d")
        # Del más reciente al más antiguo: gana el primero válido que cubra el día.
        for pub, ini, fin, path in reversed(vigencias):
            if pub <= ds and ini <= ds <= fin and valido(path):
                elegido[dia] = path
                break
        dia += timedelta(days=1)
    return elegido


def _limpiar_ceros_espurios(registros: list[dict], ventana: int = VENTANA_CEROS) -> list[dict]:
    """Quita los días con 0 trenes que son artefactos de cambio de horario.

    Un 0 puede ser dos cosas distintas y hay que tratarlas distinto:

    - **Real**: el operador no presta ese servicio. Son rachas largas de ceros y se conservan.
    - **Artefacto**: en un cambio de horario, el GTFS publicado un día tiene los servicios del
      corredor empezando al siguiente. El fichero cubre la fecha, pero ese corredor sale a 0. Son
      ceros aislados entre días normales.

    Se descartan solo los aislados: un 0 con servicio real en los `ventana` días anteriores **y**
    posteriores. Se elimina el registro en vez de inventar un valor interpolado.
    """
    por_serie: dict[tuple[str, str], dict[date, dict]] = defaultdict(dict)
    for r in registros:
        por_serie[(r["operador"], r["corredor"])][r["fecha"]] = r

    descartados = 0
    limpios = []
    for por_fecha in por_serie.values():
        for fecha, reg in por_fecha.items():
            # Se mira `trenes` (alta velocidad, la métrica que se usa) y no `trenes_todos`: en un
            # cambio de horario puede quedar algún regional suelto, y entonces `trenes_todos` vale
            # 1-2 mientras la alta velocidad está a 0, que es igual de espurio.
            if reg["trenes"] == 0:

                def hay_servicio(deltas: Iterable[int]) -> bool:
                    return any(
                        (otro := por_fecha.get(fecha + timedelta(days=d))) and otro["trenes"] > 0
                        for d in deltas
                    )

                if hay_servicio(range(-ventana, 0)) and hay_servicio(range(1, ventana + 1)):
                    descartados += 1
                    continue
            limpios.append(reg)

    if descartados:
        logger.info("descartados %d días con 0 trenes aislados (artefacto de cambio de horario)", descartados)
    return limpios


def construir(raiz: Path, desde: date, hasta: date) -> pd.DataFrame:
    """Serie diaria de oferta a partir de los ZIP que hay en `raiz/<operador>/*.zip`."""
    registros: list[dict] = []
    for carpeta in sorted(p for p in raiz.iterdir() if p.is_dir()):
        operador = carpeta.name
        zips = list(carpeta.glob("*.zip"))
        if not zips:
            continue
        logger.info("%s: leyendo vigencias de %d snapshots", operador, len(zips))
        elegido = elegir_snapshots(zips, desde, hasta)

        # Agrupar por snapshot para abrir y parsear cada ZIP una sola vez.
        por_zip: dict[Path, list[date]] = defaultdict(list)
        for dia, path in elegido.items():
            por_zip[path].append(dia)
        logger.info("  %s: %d días cubiertos por %d snapshots", operador, len(elegido), len(por_zip))

        for i, (path, dias) in enumerate(sorted(por_zip.items()), 1):
            try:
                conteos = trenes_por_dias(path, sorted(dias))
            except ERRORES_LECTURA as exc:
                logger.warning("  %s %s: ilegible (%s), se omite", operador, path.stem, exc)
                continue
            if conteos is None:
                logger.warning("  %s %s: corrupto al contar, se omite", operador, path.stem)
                continue
            for dia, por_corredor in conteos.items():
                for corredor, n in por_corredor.items():
                    registros.append({
                        "operador": operador,
                        "corredor": corredor,
                        "fecha": dia,
                        "trenes": n["alta_velocidad"],
                        "trenes_todos": n["todos"],
                        "snapshot": path.stem,
                    })
            if i % 100 == 0:
                logger.info("  %s: %d/%d snapshots procesados", operador, i, len(por_zip))

    registros = _limpiar_ceros_espurios(registros)
    df = pd.DataFrame(registros, columns=["operador", "corredor", "fecha", "trenes", "trenes_todos", "snapshot"])
    return df.sort_values(["operador", "corredor", "fecha"], ignore_index=True)


def main(argv: list[str] | None = None) -> int:
    # Las rutas del lago salen del .env, igual que en el resto del pipeline.
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Construye la serie diaria de oferta del histórico del NAP.")
    p.add_argument("--desde", type=date.fromisoformat, default=date(2025, 6, 1))
    p.add_argument("--hasta", type=date.fromisoformat, default=date.today())
    p.add_argument("--origen", type=Path, default=Path("data/historico/nap"))
    args = p.parse_args(argv)

    if not args.origen.exists():
        logger.error("no hay histórico en %s: lanza antes `make nap-historico`", args.origen)
        return 1

    df = construir(args.origen, args.desde, args.hasta)
    if df.empty:
        logger.error("no se ha podido construir ningún registro")
        return 1
    destino = referencia.escribir(
        "oferta_diaria", df, {"desde": str(args.desde), "hasta": str(args.hasta)}, os.environ, __name__
    )

    logger.info("serie de oferta: %d registros en %s", len(df), destino)
    corredor = df[df["corredor"] == "Madrid-Barcelona"]
    for operador, grupo in corredor.groupby("operador"):
        logger.info(
            "  %s Madrid-Barcelona: %d días, %s .. %s, media %.1f trenes/día",
            operador, len(grupo), grupo["fecha"].min(), grupo["fecha"].max(), grupo["trenes"].mean(),
        )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
