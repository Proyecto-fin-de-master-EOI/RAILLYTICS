"""Tests de la caché de snapshots del NAP.

Lo que se vigila es una sola cosa, porque es la que justifica que la caché exista: **la serie que
sale con caché tiene que ser la misma que sin ella**. Una caché que acierta cuando no debería no da
error, da una serie vieja; y eso en un cuadro de mandos se lee como un dato.
"""
import json
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from raillytics.ingesta import nap_oferta
from raillytics.ingesta.nap_cache import CacheSnapshots, SinCache

from test_nap_oferta import _gtfs_simple  # el mismo GTFS mínimo que el resto de los tests


def _historico(tmp_path: Path, publicaciones: list[tuple[str, str, str]]) -> Path:
    """Un histórico con un operador y un ZIP por publicación: (fecha_pub, inicio, fin)."""
    carpeta = tmp_path / "nap" / "renfe"
    carpeta.mkdir(parents=True)
    for pub, inicio, fin in publicaciones:
        _gtfs_simple(carpeta / f"{pub}_1.zip", inicio=inicio, fin=fin)
    return tmp_path / "nap"


def _serie(raiz, desde, hasta, cache=None):
    df, franjas = nap_oferta.construir(raiz, desde, hasta, cache)
    return df, franjas


# --------------------------------------------------------------------------- lo que de verdad importa

def test_la_serie_con_cache_es_la_misma_que_sin_cache(tmp_path):
    raiz = _historico(tmp_path, [("2026-01-01", "20260101", "20260131")])
    desde, hasta = date(2026, 1, 1), date(2026, 1, 10)

    sin, sin_franjas = _serie(raiz, desde, hasta)
    cache = CacheSnapshots(tmp_path / "cache")
    frio, frio_franjas = _serie(raiz, desde, hasta, cache)       # llena la caché
    caliente, caliente_franjas = _serie(raiz, desde, hasta, cache)  # ya la usa

    pd.testing.assert_frame_equal(sin, frio)
    pd.testing.assert_frame_equal(sin, caliente)
    pd.testing.assert_frame_equal(sin_franjas, caliente_franjas)
    assert cache.aciertos > 0


def test_la_segunda_pasada_no_vuelve_a_abrir_los_zip(tmp_path, monkeypatch):
    """Es el motivo de ser de la caché: 1,45 s por snapshot de Renfe y 485 en el histórico."""
    raiz = _historico(tmp_path, [("2026-01-01", "20260101", "20260131")])
    cache = CacheSnapshots(tmp_path / "cache")
    _serie(raiz, date(2026, 1, 1), date(2026, 1, 10), cache)

    def no_parsear(*a, **k):
        raise AssertionError("ha vuelto a parsear un ZIP que ya estaba en la caché")

    monkeypatch.setattr(nap_oferta, "trenes_por_dias", no_parsear)
    monkeypatch.setattr(nap_oferta, "vigencia", no_parsear)
    monkeypatch.setattr(nap_oferta, "coherente", no_parsear)
    df, _ = _serie(raiz, date(2026, 1, 1), date(2026, 1, 10), cache)
    assert df["fecha"].nunique() == 10


def test_un_zip_que_cambia_de_contenido_no_acierta(tmp_path):
    """La clave lleva tamaño y fecha de modificación. Si acertara, la serie se quedaría con el
    horario viejo de un snapshot que se volvió a descargar distinto."""
    raiz = _historico(tmp_path, [("2026-01-01", "20260101", "20260131")])
    cache = CacheSnapshots(tmp_path / "cache")
    antes, _ = _serie(raiz, date(2026, 1, 1), date(2026, 1, 5), cache)

    # Se reescribe el mismo nombre con un GTFS que no cubre esas fechas.
    _gtfs_simple(raiz / "renfe" / "2026-01-01_1.zip", inicio="20260601", fin="20260630")
    despues, _ = _serie(raiz, date(2026, 1, 1), date(2026, 1, 5), cache)

    assert antes["fecha"].nunique() == 5
    assert despues.empty


@pytest.mark.parametrize("snapshots", [3, 6, 12])
def test_el_coste_de_una_pasada_diaria_no_crece_con_el_historico(tmp_path, snapshots):
    """El caso real y la razón de ser de todo esto.

    Sin caché, añadir el snapshot de hoy obliga a reparsear los 485 del histórico: 14 min para un
    fichero nuevo. Con caché, una pasada diaria cuesta **lo mismo da cuánto histórico haya**: el ZIP
    nuevo (vigencia, coherencia y conteos) y los conteos del que hasta ahora cerraba la serie, al que
    le cambian los días que le tocan. Cuatro cálculos, no 3 por snapshot.
    """
    carpeta = tmp_path / "nap" / "renfe"
    carpeta.mkdir(parents=True)
    for i in range(1, snapshots + 1):
        _gtfs_simple(carpeta / f"2026-01-{i:02d}_1.zip", inicio="20260101", fin="20260131")
    raiz = tmp_path / "nap"
    cache = CacheSnapshots(tmp_path / "cache")
    _serie(raiz, date(2026, 1, 1), date(2026, 1, 25), cache)
    assert cache.fallos == 3 * snapshots      # la primera vez se paga todo

    _gtfs_simple(carpeta / f"2026-01-{snapshots + 1:02d}_9.zip", inicio="20260101", fin="20260131")
    cache.fallos = cache.aciertos = 0
    df, _ = _serie(raiz, date(2026, 1, 1), date(2026, 1, 25), cache)

    assert df["fecha"].nunique() == 25
    assert cache.fallos == 4
    assert cache.aciertos == 3 * snapshots - 1


# --------------------------------------------------------------------------- mejor esfuerzo

def test_un_directorio_que_no_se_puede_escribir_no_rompe_la_serie(tmp_path, monkeypatch):
    raiz = _historico(tmp_path, [("2026-01-01", "20260101", "20260131")])
    cache = CacheSnapshots(tmp_path / "cache")
    monkeypatch.setattr(Path, "mkdir", lambda *a, **k: (_ for _ in ()).throw(OSError("solo lectura")))

    df, _ = _serie(raiz, date(2026, 1, 1), date(2026, 1, 5), cache)
    assert df["fecha"].nunique() == 5
    assert cache.aciertos == 0


def test_una_entrada_corrupta_se_recalcula(tmp_path):
    raiz = _historico(tmp_path, [("2026-01-01", "20260101", "20260131")])
    directorio = tmp_path / "cache"
    cache = CacheSnapshots(directorio)
    esperado, _ = _serie(raiz, date(2026, 1, 1), date(2026, 1, 5), cache)

    for fichero in directorio.glob("*.json"):
        fichero.write_text("{esto no es json", encoding="utf-8")
    cache.aciertos = 0
    df, _ = _serie(raiz, date(2026, 1, 1), date(2026, 1, 5), cache)

    assert cache.aciertos == 0
    pd.testing.assert_frame_equal(esperado, df)


def test_un_valor_nulo_se_cachea_igual(tmp_path):
    """Un snapshot corrupto devuelve None y eso también vale la pena recordarlo: si no, se
    reparsearía el fichero malo en cada pasada."""
    cache = CacheSnapshots(tmp_path / "cache")
    z = _gtfs_simple(tmp_path / "x_1.zip", inicio="20260101", fin="20260131")

    llamadas = []

    def calcular():
        llamadas.append(1)
        return None

    assert cache.recordar(z, "prueba", (), calcular) is None
    assert cache.recordar(z, "prueba", (), calcular) is None
    assert len(llamadas) == 1


def test_sin_cache_no_guarda_nada_y_devuelve_lo_calculado(tmp_path):
    cache = SinCache()
    z = _gtfs_simple(tmp_path / "x_1.zip", inicio="20260101", fin="20260131")
    assert cache.recordar(z, "prueba", (), lambda: 42) == 42
    assert cache.recordar(z, "prueba", (), lambda: 43) == 43
    assert list(tmp_path.glob("*.json")) == []


def test_el_formato_invalida_lo_guardado(tmp_path, monkeypatch):
    """Si cambia lo que significa una entrada, subir FORMATO tiene que dejar de acertar."""
    from raillytics.ingesta import nap_cache

    z = _gtfs_simple(tmp_path / "x_1.zip", inicio="20260101", fin="20260131")
    cache = CacheSnapshots(tmp_path / "cache")
    cache.recordar(z, "prueba", (), lambda: "viejo")

    monkeypatch.setattr(nap_cache, "FORMATO", nap_cache.FORMATO + 1)
    assert cache.recordar(z, "prueba", (), lambda: "nuevo") == "nuevo"


def test_un_zip_que_desaparece_no_tira_la_clave(tmp_path):
    cache = CacheSnapshots(tmp_path / "cache")
    assert cache.recordar(tmp_path / "no-existe.zip", "prueba", (), lambda: 1) == 1
