"""¿Se puede contrastar `base_dia_semana` con el interés de búsqueda de Google Trends?

Los coeficientes de `base_dia_semana` en `config/reglas_demanda.yml` se calibraron contra la **oferta**
del NAP: cuántos trenes pone el operador cada día. Eso mide el techo de capacidad, no la demanda, y el
propio fichero lo dice («lo medido es el TECHO, no el valor»). Este script busca un contraste del lado
de la **demanda** y además independiente: cuánta gente busca el viaje cada día de la semana.

**Por qué esto se puede hacer y la serie larga no.** Google Trends normaliza cada descarga poniendo el
máximo de la ventana pedida a 100, así que dos ventanas no son comparables y construir histórico
obliga a encadenarlas con solapes, acumulando error. Pero **para un perfil semanal no hace falta
encadenar nada**: la pregunta «¿qué día se busca más?» se contesta dentro de una sola ventana, donde
la normalización es la misma para todos los días y por tanto da igual. Esa es la razón de que este
contraste sea viable y el histórico diario no.

La granularidad la decide el rango pedido, no quien pregunta: menos de 9 meses da días, de 9 meses a
5 años semanas, y más de 5 años meses. Por eso las ventanas son de 260 días.

**No entra en el registro de fuentes ni en el pipeline, a propósito.** Es un endpoint no documentado,
devuelve una muestra aleatoria (dos peticiones idénticas dan números distintos) y redondea a entero,
así que una tabla construida con esto no sería reproducible y rompería la trazabilidad del lago. Se
usa como experimento, se apunta el resultado y se puede relanzar.

Uso:
    python scripts/contraste_trends.py                 # tres ventanas, una por año
    python scripts/contraste_trends.py --ventanas 2026-01-14:2026-09-30
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from statistics import mean, median, stdev

import numpy as np
import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))

from raillytics.ingesta.referencia import conexion  # noqa: E402
from raillytics.prediccion.calibracion import oferta_diaria  # noqa: E402
from raillytics.prediccion.reglas import cargar_reglas  # noqa: E402
from raillytics.utils.lake import LakeLayout  # noqa: E402

logger = logging.getLogger(__name__)

BASE = "https://trends.google.com/trends/api"
# Trends responde con un prefijo anti-XSSI antes del JSON.
PREFIJO = ")]}'"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/131.0.0.0 Safari/537.36")
CONSULTA = "tren madrid barcelona"
GEO = "ES"
IDIOMA = "es-ES"
# Menos de 9 meses (~269 días) o Google agrega por semanas y el perfil semanal desaparece.
DIAS_VENTANA = 260
SEMANA = ("lun", "mar", "mié", "jue", "vie", "sáb", "dom")
# El martes vale 1.00 en las reglas, así que todo se normaliza contra él.
REFERENCIA = 1
# Un pico aislado no es demanda recurrente: una noticia o una huelga. Además fija el 100 de la
# ventana y comprime todo lo demás, así que distorsiona el perfil entero.
UMBRAL_PICO = 3.0
# Con siete puntos (uno por día) hace falto |r| > 0.75 para hablar de correlación al 5 %.
R_MINIMO = 0.75
ESPERA_ENTRE_VENTANAS = 4.0


def _sin_prefijo(texto: str) -> dict:
    return json.loads(texto[texto.index("{"):])


def serie_diaria(sesion: requests.Session, desde: date, hasta: date, consulta: str = CONSULTA) -> list[tuple[date, float]]:
    """Interés de búsqueda por día en la ventana pedida, 0-100 relativo a su propio máximo."""
    ventana = f"{desde.isoformat()} {hasta.isoformat()}"
    peticion = {"comparisonItem": [{"keyword": consulta, "geo": GEO, "time": ventana}],
                "category": 0, "property": ""}
    comun = {"hl": IDIOMA, "tz": "-120"}

    r = sesion.get(f"{BASE}/explore", params={**comun, "req": json.dumps(peticion)}, timeout=40)
    r.raise_for_status()
    widgets = _sin_prefijo(r.text)["widgets"]
    serie_widget = next((w for w in widgets if w.get("id") == "TIMESERIES"), None)
    if serie_widget is None:
        raise RuntimeError("Trends no ha devuelto el widget TIMESERIES: ha cambiado la respuesta")

    resolucion = serie_widget["request"].get("resolution")
    if resolucion != "DAY":
        raise RuntimeError(
            f"Trends devuelve resolución {resolucion} y no DAY para una ventana de "
            f"{(hasta - desde).days + 1} días: hay que pedir menos de 9 meses"
        )

    r = sesion.get(f"{BASE}/widgetdata/multiline",
                   params={**comun, "req": json.dumps(serie_widget["request"]), "token": serie_widget["token"]},
                   timeout=40)
    r.raise_for_status()
    filas = _sin_prefijo(r.text)["default"]["timelineData"]
    return [(date.fromtimestamp(int(f["time"])), float(f["value"][0]))
            for f in filas if f.get("hasData", [True])[0]]


def perfil_semanal(serie: list[tuple[date, float]]) -> tuple[dict[int, float], dict[int, float], list[tuple[date, float]]]:
    """Media por día relativa al martes, su contraste contra el resto de la semana, y los picos."""
    nivel = median([v for _, v in serie])
    picos = [(d, v) for d, v in serie if v > nivel * UMBRAL_PICO]
    limpia = [(d, v) for d, v in serie if v <= nivel * UMBRAL_PICO]

    por_dia: dict[int, list[float]] = {}
    for d, v in limpia:
        por_dia.setdefault(d.weekday(), []).append(v)
    if len(por_dia) < 7:
        raise RuntimeError("la ventana no cubre los siete días de la semana")

    # El perfil se expresa con el martes en 1.00 solo para poder ponerlo al lado de
    # `base_dia_semana`, que usa esa unidad. Es un CAMBIO DE UNIDAD, no una afirmación: dividir por
    # otro día deja idénticas todas las proporciones entre días, y el nivel absoluto no lo usa nadie
    # porque `normalizar_indices` reescala el reparto a media 1.0 antes de aplicar el total de CNMC.
    perfil = {i: mean(por_dia[i]) / mean(por_dia[REFERENCIA]) for i in range(7)}

    # El contraste, en cambio, NO se hace contra un día elegido: cada día se compara con los otros
    # seis juntos. Si se usara el martes como referencia, su propio ruido contaminaría las siete
    # comparaciones —y pasó: un pico en martes dejó toda la semana en «no significativo»—.
    t = {}
    for i in range(7):
        suyos = por_dia[i]
        resto = [v for j in range(7) if j != i for v in por_dia[j]]
        e = (stdev(suyos) ** 2 / len(suyos) + stdev(resto) ** 2 / len(resto)) ** 0.5
        t[i] = (mean(suyos) - mean(resto)) / e if e else 0.0
    return perfil, t, picos


def referencias(con, layout: LakeLayout, ruta_reglas: Path) -> tuple[dict[int, float], dict[int, float]]:
    """Los dos perfiles con los que comparar: las reglas actuales y la oferta medida del NAP."""
    reglas = cargar_reglas(ruta_reglas)
    regla = {i: reglas.base_dia_semana[SEMANA[i]] for i in range(7)}

    oferta = oferta_diaria(con, layout)
    if oferta.empty:
        return regla, {}
    oferta = oferta.assign(dia=pd.to_datetime(oferta["fecha"]).dt.weekday)
    medianas = oferta.groupby("dia")["trenes"].median()
    nap = {i: medianas[i] / medianas[REFERENCIA] for i in range(7)}
    return regla, nap


def desfases(perfil: dict[int, float], otro: dict[int, float]) -> list[tuple[int, float]]:
    """Correlación del perfil rotado k días con otro perfil, para k de 0 a 6.

    Si buscar adelanta a viajar k días, el perfil de búsquedas rotado k posiciones debería encajar
    con el de viajes. Que ninguna rotación encaje significa que el desfase no se puede identificar
    con un ciclo semanal, y entonces el contraste no concluye nada.
    """
    if not otro:
        return []
    objetivo = [otro[i] for i in range(7)]
    salida = []
    for k in range(7):
        rotado = [perfil[(i - k) % 7] for i in range(7)]
        salida.append((k, float(np.corrcoef(rotado, objetivo)[0, 1])))
    return salida


def informe(ventanas: list[tuple[date, date, dict[int, float], dict[int, float], list]],
            regla: dict[int, float], nap: dict[int, float]) -> list[str]:
    out = ["CONTRASTE DE base_dia_semana CON EL INTERÉS DE BÚSQUEDA", "",
           f"   consulta: «{CONSULTA}»   geo: {GEO}",
           f"   {len(ventanas)} ventanas de {DIAS_VENTANA} días (menos de 9 meses para que Google dé días)", ""]

    for desde, hasta, perfil, t, picos in ventanas:
        out.append(f"   --- {desde} a {hasta} ---")
        if picos:
            out.append("   picos excluidos (> %.0fx la mediana: una noticia, no demanda recurrente): %s"
                       % (UMBRAL_PICO, ", ".join(f"{d} {SEMANA[d.weekday()]} {v:.0f}" for d, v in picos)))
        out.append(f"   {'día':5} {'vs martes':>10} {'t':>7}  ¿se distingue del resto de la semana?")
        for i, nombre in enumerate(SEMANA):
            out.append(f"   {nombre:5} {perfil[i]:>10.3f} {t[i]:>+7.1f}  {'SÍ' if abs(t[i]) >= 2 else 'no'}")
        out.append("")

    out += ["   ESTABILIDAD ENTRE VENTANAS (Trends es una muestra aleatoria: cada descarga difiere)", "",
            f"   {'día':5} " + " ".join(f"{d.year:>8}" for d, _, _, _, _ in ventanas)
            + f" {'media':>8} {'reglas':>8} {'oferta':>8}"]
    medias = {}
    for i, nombre in enumerate(SEMANA):
        valores = [p[i] for _, _, p, _, _ in ventanas]
        medias[i] = mean(valores)
        fila = f"   {nombre:5} " + " ".join(f"{v:>8.3f}" for v in valores) + f" {medias[i]:>8.3f} {regla[i]:>8.2f}"
        out.append(fila + (f" {nap[i]:>8.2f}" if nap else f" {'-':>8}"))

    amplitud = max(medias.values()) / min(medias.values())
    out += ["", f"   amplitud del perfil semanal: búsquedas {amplitud:.2f}x   "
                f"reglas {max(regla.values())/min(regla.values()):.2f}x"
                + (f"   oferta {max(nap.values())/min(nap.values()):.2f}x" if nap else "")]

    for etiqueta, otro in (("las reglas", regla), ("la oferta del NAP", nap)):
        corr = desfases(medias, otro)
        if not corr:
            continue
        out += ["", f"   DESFASE frente a {etiqueta}: correlación del perfil rotado k días",
                f"   (con 7 puntos hace falta |r| > {R_MINIMO} para hablar de correlación al 5 %)", ""]
        out.append("   " + "  ".join(f"k={k}: {r:+.2f}" for k, r in corr))
        mejor = max(corr, key=lambda x: x[1])
        veredicto = ("hay un desfase compatible" if mejor[1] >= R_MINIMO
                     else "NINGUNA rotación encaja: el desfase no se puede identificar")
        out.append(f"   mejor k = {mejor[0]} con r = {mejor[1]:+.2f}  ->  {veredicto}")
    return out


def main(argv: list[str] | None = None) -> int:
    load_dotenv(find_dotenv(usecwd=True))
    p = argparse.ArgumentParser(description="Contrasta base_dia_semana con el interés de búsqueda.")
    p.add_argument("--reglas", type=Path, default=Path("config/reglas_demanda.yml"))
    p.add_argument("--consulta", default=CONSULTA)
    p.add_argument("--ventanas", nargs="*", default=None,
                   help="ventanas DESDE:HASTA; por defecto una por los tres últimos años")
    p.add_argument("--hasta", type=date.fromisoformat, default=None)
    args = p.parse_args(argv)

    if args.ventanas:
        rangos = [tuple(date.fromisoformat(x) for x in v.split(":")) for v in args.ventanas]
    else:
        fin = args.hasta or (date.today() - timedelta(days=9))  # Trends va con unos días de retraso
        rangos = [(date(fin.year - n, fin.month, fin.day) - timedelta(days=DIAS_VENTANA - 1),
                   date(fin.year - n, fin.month, fin.day)) for n in (2, 1, 0)]

    sesion = requests.Session()
    sesion.headers["User-Agent"] = UA
    sesion.get(f"https://trends.google.com/trends/?geo={GEO}", timeout=40)

    ventanas = []
    for i, (desde, hasta) in enumerate(rangos):
        if i:
            time.sleep(ESPERA_ENTRE_VENTANAS)  # Trends responde 429 con facilidad
        logger.info("%s a %s", desde, hasta)
        serie = serie_diaria(sesion, desde, hasta, args.consulta)
        perfil, t, picos = perfil_semanal(serie)
        ventanas.append((desde, hasta, perfil, t, picos))

    layout = LakeLayout.from_env(os.environ)
    regla, nap = referencias(conexion(layout, os.environ), layout, args.reglas)
    if not nap:
        logger.warning("no hay oferta en Silver: se compara solo con las reglas (`make nap-oferta`)")

    print("\n".join(informe(ventanas, regla, nap)))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    raise SystemExit(main())
