import math
from datetime import date

import pandas as pd

from raillytics.prediccion.calendario import construir_calendario, lineas_prompt
from raillytics.prediccion.trimestre import Trimestre

T4_2026 = Trimestre(2026, 4)


def festivos(*filas):
    return pd.DataFrame(list(filas), columns=["fecha", "nombre"])


def eventos(*filas):
    return pd.DataFrame(list(filas), columns=["fecha", "descripcion", "ciudad"])


def meteo(*filas):
    return pd.DataFrame(list(filas), columns=["fecha", "ciudad", "temperatura_media", "precipitacion_mm"])


def _fila(cal, dia):
    return cal.dias[cal.dias["fecha"] == dia].iloc[0]


def test_cubre_todos_los_dias_del_trimestre_con_su_dia_de_la_semana():
    cal = construir_calendario(T4_2026, festivos(), eventos(), meteo())

    assert len(cal.dias) == 92
    assert list(cal.dias["fecha"]) == T4_2026.dias()
    assert _fila(cal, date(2026, 10, 1))["dia_semana"] == "jue"
    assert _fila(cal, date(2026, 12, 25))["dia_semana"] == "vie"


def test_un_trimestre_de_anio_bisiesto_incluye_el_29_de_febrero():
    cal = construir_calendario(Trimestre(2028, 1), festivos(), eventos(), meteo())

    assert len(cal.dias) == 91 and _fila(cal, date(2028, 2, 29))["dia_semana"] == "mar"


def test_marca_los_festivos():
    cal = construir_calendario(T4_2026, festivos((date(2026, 12, 25), "Navidad")), eventos(), meteo())

    assert _fila(cal, date(2026, 12, 25))["festivo"] == "Navidad"
    assert (cal.dias["festivo"] != "").sum() == 1


def test_agrupa_los_eventos_de_un_dia_en_orden_y_ignora_los_de_fuera_del_trimestre():
    cal = construir_calendario(
        T4_2026,
        festivos(),
        eventos(
            (date(2026, 11, 29), "Partido de liga", "MAD"),
            (date(2026, 11, 29), "Concierto", ""),
            (date(2027, 2, 1), "Evento de otro trimestre", "BCN"),
        ),
        meteo(),
    )

    assert _fila(cal, date(2026, 11, 29))["eventos"] == "Concierto ; Partido de liga (MAD)"
    assert (cal.dias["eventos"] != "").sum() == 1
    assert cal.eventos_con_datos is True


def test_sin_eventos_dentro_del_trimestre_no_hay_datos_de_eventos():
    solo_fuera = eventos((date(2027, 2, 1), "Evento de otro trimestre", "BCN"))

    assert construir_calendario(T4_2026, festivos(), solo_fuera, meteo()).eventos_con_datos is False
    assert construir_calendario(T4_2026, festivos(), eventos(), meteo()).eventos_con_datos is False


def test_el_texto_de_los_eventos_se_sanea_para_el_prompt():
    cal = construir_calendario(
        T4_2026, festivos(), eventos((date(2026, 11, 29), "Final | Copa\n  del Rey", "BCN")), meteo()
    )

    assert _fila(cal, date(2026, 11, 29))["eventos"] == "Final / Copa del Rey (BCN)"


def test_meteo_usa_lo_observado_y_si_no_hay_la_climatologia_del_mes():
    observada = meteo(
        (date(2025, 12, 1), "MAD", 10.0, 0.0),
        (date(2025, 12, 2), "MAD", 8.0, 2.0),
        (date(2026, 12, 1), "MAD", 6.0, 4.0),
    )

    cal = construir_calendario(T4_2026, festivos(), eventos(), observada)

    observado = _fila(cal, date(2026, 12, 1))
    assert (observado["temp_MAD"], observado["prec_MAD"], observado["origen_MAD"]) == (6.0, 4.0, "observado")
    clima = _fila(cal, date(2026, 12, 5))  # media de diciembre: (10 + 8 + 6) / 3 y (0 + 2 + 4) / 3
    assert (clima["temp_MAD"], clima["prec_MAD"], clima["origen_MAD"]) == (8.0, 2.0, "climatologia")
    sin_historia = _fila(cal, date(2026, 10, 5))
    assert sin_historia["origen_MAD"] == "sin_datos" and math.isnan(sin_historia["temp_MAD"])
    assert set(cal.dias["origen_BCN"]) == {"sin_datos"}


def test_las_lineas_del_prompt_resumen_cada_dia():
    cal = construir_calendario(
        T4_2026,
        festivos((date(2026, 12, 25), "Navidad")),
        eventos((date(2026, 11, 29), "Real Madrid–Barça", "MAD")),
        meteo((date(2026, 12, 1), "MAD", 6.0, 4.0), (date(2025, 12, 1), "BCN", 12.0, 0.0)),
    )

    lineas = lineas_prompt(cal)
    por_fecha = {linea.split(" | ")[0]: linea for linea in lineas}

    assert len(lineas) == 92
    assert por_fecha["2026-12-25"].startswith("2026-12-25 | vie | festivo: Navidad | eventos: ninguno | meteo: ")
    assert por_fecha["2026-12-25"].endswith("meteo: MAD 6°C 4.0mm (clim.); BCN 12°C 0.0mm (clim.)")
    assert "festivo: no" in por_fecha["2026-10-01"]
    assert "eventos: Real Madrid–Barça (MAD)" in por_fecha["2026-11-29"]
    assert "MAD 6°C 4.0mm (obs.)" in por_fecha["2026-12-01"]
    assert por_fecha["2026-10-05"].endswith("meteo: MAD sin datos; BCN sin datos")


def test_sin_datos_de_eventos_las_lineas_lo_dicen_en_vez_de_ninguno():
    cal = construir_calendario(T4_2026, festivos(), eventos(), meteo())

    assert all("eventos: sin datos" in linea for linea in lineas_prompt(cal))
