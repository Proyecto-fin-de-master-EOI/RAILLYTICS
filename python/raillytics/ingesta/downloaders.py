"""Cómo se trae cada fuente: la clave `downloader` de config/data_sources.yml.

Por defecto `http`, que es un GET de `url` y vale para la mayoría. Dos fuentes no
caben ahí y aportan el suyo, porque su adquisición no es una petición sino un
procedimiento:

  aemet  La API responde con un enlace temporal, no con los datos (dos pasos), y
         limita cada petición a ~15 días, así que la URL lleva el rango de fechas.
  nap    Hay que listar los snapshots del conjunto, elegir uno y pedir su enlace
         de descarga, que es una URL firmada de S3 que caduca (tres pasos).

La alternativa era meter plantillas de URL, selectores y rangos en el YAML, es
decir, convertir un fichero de configuración en un lenguaje de programación. Con
esto las fuentes simples siguen siendo cuatro líneas y las complejas tienen
código, que se puede testear, reintentar y leer.

La credencial nunca está en el YAML: `auth.env` es el NOMBRE de una variable de
entorno (config/data_sources.yml está versionado, el .env no).
"""
from __future__ import annotations

import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta

import requests

from raillytics.ingesta.sources import DataSource

_TIMEOUT_SECONDS = 60

# AEMET publica la climatología con unos días de retardo, así que pedir "hasta hoy"
# devuelve un tramo vacío. Se pide una ventana que termina hace DIAS_RETARDO días.
AEMET_DIAS_RETARDO = 5
# La API rechaza rangos largos; 15 días es el máximo que admite de una vez.
AEMET_DIAS_VENTANA = 15
# El segundo paso de AEMET (la URL temporal) devuelve 500 de forma intermitente: comprobado que
# el mismo enlace funciona al reintentar. Sin esto, un 500 esporádico tira la tarea del DAG.
AEMET_REINTENTOS = 3
AEMET_ESPERA_SEGUNDOS = 3


@dataclass(frozen=True)
class Respuesta:
    """Lo que devuelve un descargador: el contenido y las cabeceras que describen el fichero.

    Las cabeceras solo se usan para nombrar el fichero en el staging y para el
    quality gate de `content_type`, así que un descargador puede sintetizarlas.
    """

    content: bytes
    headers: Mapping[str, str]


def obtener(source: DataSource) -> Respuesta:
    """Trae la fuente con el descargador que declare, `http` si no declara ninguno."""
    return _DESCARGADORES[source.downloader](source)


def _credencial(source: DataSource) -> str:
    variable = source.auth.get("env")
    if not variable:
        raise RuntimeError(f"La fuente '{source.id}' usa el descargador '{source.downloader}' pero no declara 'auth.env'")
    valor = os.getenv(variable)
    if not valor:
        raise RuntimeError(
            f"Falta la variable de entorno {variable}, que necesita la fuente '{source.id}'. "
            f"Se define en el .env del proyecto (ver .env.example)."
        )
    return valor


def _cabecera_auth(source: DataSource) -> dict[str, str]:
    return {source.auth.get("header", "ApiKey"): _credencial(source)}


def _nombre(fichero: str) -> dict[str, str]:
    """Content-Disposition sintético, para que el fichero conserve su nombre en el staging."""
    return {"Content-Disposition": f'attachment; filename="{fichero}"'}


def _http(source: DataSource) -> Respuesta:
    """Descarga directa de `url`. El caso normal."""
    cabeceras = _cabecera_auth(source) if source.auth else {}
    r = requests.get(source.url, headers=cabeceras or None, timeout=_TIMEOUT_SECONDS)
    r.raise_for_status()
    return Respuesta(content=r.content, headers=r.headers)


def _aemet(source: DataSource) -> Respuesta:
    """AEMET OpenData: la petición devuelve un enlace temporal y de ahí se bajan los datos.

    `url` es una plantilla con {fecha_ini} y {fecha_fin}; aquí se rellenan con una
    ventana de AEMET_DIAS_VENTANA días que termina hace AEMET_DIAS_RETARDO.
    """
    fin = date.today() - timedelta(days=AEMET_DIAS_RETARDO)
    ini = fin - timedelta(days=AEMET_DIAS_VENTANA - 1)
    url = source.url.format(fecha_ini=ini.isoformat(), fecha_fin=fin.isoformat())

    cabeceras = {source.auth.get("header", "api_key"): _credencial(source)}
    r = requests.get(url, headers=cabeceras, timeout=_TIMEOUT_SECONDS)
    r.raise_for_status()
    sobre = r.json()
    if sobre.get("estado") != 200 or not sobre.get("datos"):
        raise RuntimeError(f"AEMET rechazó la petición de '{source.id}': {sobre.get('descripcion') or sobre}")

    datos = _con_reintentos(sobre["datos"], AEMET_REINTENTOS, AEMET_ESPERA_SEGUNDOS)
    return Respuesta(content=datos.content, headers={**datos.headers, **_nombre(f"{source.id}_{ini}_{fin}.json")})


def _con_reintentos(url: str, intentos: int, espera: int) -> requests.Response:
    """GET con reintentos y espera creciente. El último fallo se propaga tal cual."""
    for intento in range(1, intentos + 1):
        r = requests.get(url, timeout=_TIMEOUT_SECONDS)
        if r.ok:
            return r
        if intento == intentos:
            r.raise_for_status()
        time.sleep(espera * intento)
    raise AssertionError("inalcanzable")  # pragma: no cover


def _nap(source: DataSource) -> Respuesta:
    """NAP: lista los snapshots del conjunto, coge el más reciente y sigue su enlace firmado.

    `url` es el endpoint del histórico del conjunto. El `enlaceDescarga` que
    devuelve el listado trae los dos últimos segmentos invertidos y da 404
    (comprobado), así que se reconstruye la ruta correcta a partir del id.
    """
    cabeceras = _cabecera_auth(source)
    listado = requests.get(source.url, headers=cabeceras, timeout=_TIMEOUT_SECONDS)
    listado.raise_for_status()
    snapshots = [s for s in listado.json().get("data", []) if s.get("fecha") and s.get("id")]
    if not snapshots:
        raise RuntimeError(f"El NAP no devolvió ningún snapshot para '{source.id}'")
    ultimo = max(snapshots, key=lambda s: str(s["fecha"]))

    base = source.url.split("/api/v2/")[0]
    enlace = requests.get(
        f"{base}/api/v2/fichero/{ultimo['id']}/descarga/historico", headers=cabeceras, timeout=_TIMEOUT_SECONDS
    )
    enlace.raise_for_status()
    datos = enlace.json().get("data") or {}
    if not datos.get("enlaceDescarga"):
        raise RuntimeError(f"El NAP no devolvió enlace de descarga para el snapshot {ultimo['id']} de '{source.id}'")

    # La URL firmada de S3 ya lleva la autorización en la propia firma: no se le manda la ApiKey.
    fichero = requests.get(datos["enlaceDescarga"], timeout=_TIMEOUT_SECONDS)
    fichero.raise_for_status()
    nombre = datos.get("nombreFichero") or f"{ultimo['nombreArchivo']}.zip"
    return Respuesta(content=fichero.content, headers={**fichero.headers, **_nombre(nombre)})


_DESCARGADORES = {"http": _http, "aemet": _aemet, "nap": _nap}
