"""El perfil semanal de Trends y la identificación del desfase.

El script se carga por ruta, como `test_carga_e2e.py`: vive en `scripts/` y no es un paquete.
"""
from __future__ import annotations

import importlib.util
from datetime import date, timedelta
from pathlib import Path

import pytest

RUTA = Path(__file__).resolve().parents[2] / "scripts" / "contraste_trends.py"
spec = importlib.util.spec_from_file_location("contraste_trends", RUTA)
ct = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ct)


def _serie(valores_por_dia: dict[int, float], semanas: int = 6, desde=date(2026, 1, 5)) -> list:
    """Una serie diaria en la que cada día de la semana vale siempre lo mismo. El 5-ene-2026 es lunes."""
    serie = []
    for s in range(semanas):
        for i in range(7):
            d = desde + timedelta(days=s * 7 + i)
            serie.append((d, valores_por_dia[d.weekday()]))
    return serie


def test_la_ventana_pedida_tiene_que_dar_resolucion_diaria():
    # Es la trampa del script: a partir de ~269 días Google agrega por semanas y el perfil semanal
    # desaparece sin dar ningún error.
    assert ct.DIAS_VENTANA < 269


def test_el_perfil_se_mide_contra_el_martes():
    perfil, _, _ = ct.perfil_semanal(_serie({0: 20.0, 1: 20.0, 2: 20.0, 3: 20.0, 4: 20.0, 5: 10.0, 6: 30.0}))

    assert perfil[ct.REFERENCIA] == 1.0
    assert perfil[6] == pytest.approx(1.5)   # domingo, 30 sobre 20
    assert perfil[5] == pytest.approx(0.5)   # sábado, 10 sobre 20


def test_un_pico_aislado_se_excluye_porque_fija_el_100_de_la_ventana():
    serie = _serie({i: 20.0 for i in range(7)})
    # Un lunes con una noticia: 100 es 5 veces la mediana.
    serie[0] = (serie[0][0], 100.0)

    perfil, _, picos = ct.perfil_semanal(serie)

    assert [v for _, v in picos] == [100.0]
    assert perfil[0] == pytest.approx(1.0)  # sin el pico, el lunes es como el resto


def test_un_pico_que_no_llega_al_umbral_se_queda():
    serie = _serie({i: 20.0 for i in range(7)})
    serie[0] = (serie[0][0], 50.0)  # 2.5x la mediana, por debajo del umbral de 3x

    perfil, _, picos = ct.perfil_semanal(serie)

    assert picos == []
    assert perfil[0] > 1.0


def test_una_ventana_que_no_cubre_la_semana_entera_falla():
    corta = _serie({i: 20.0 for i in range(7)})[:4]

    with pytest.raises(RuntimeError, match="siete días"):
        ct.perfil_semanal(corta)


def test_el_desfase_cero_de_un_perfil_contra_si_mismo_es_correlacion_uno():
    perfil = {0: 1.05, 1: 1.00, 2: 0.97, 3: 0.99, 4: 0.94, 5: 0.92, 6: 1.11}

    corr = dict(ct.desfases(perfil, perfil))

    assert len(corr) == 7
    assert corr[0] == pytest.approx(1.0)


def test_un_perfil_desplazado_se_detecta_en_su_desfase():
    # Si el perfil de búsquedas es el de viajes adelantado 2 días, k=2 tiene que dar correlación 1.
    viajes = {0: 1.05, 1: 1.00, 2: 0.97, 3: 0.99, 4: 0.94, 5: 0.92, 6: 1.11}
    busquedas = {i: viajes[(i + 2) % 7] for i in range(7)}

    corr = dict(ct.desfases(busquedas, viajes))

    assert corr[2] == pytest.approx(1.0)
    assert max(corr, key=corr.get) == 2


def test_sin_oferta_en_silver_no_se_inventa_un_desfase():
    perfil = {i: 1.0 for i in range(7)}

    assert ct.desfases(perfil, {}) == []
