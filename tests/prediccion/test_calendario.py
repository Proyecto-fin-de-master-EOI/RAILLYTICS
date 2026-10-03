import math
from datetime import date

import pandas as pd

from raillytics.prediccion.calendario import construir_calendario, lineas_prompt, tipos_de_dia
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


def test_sin_climatologia_las_lineas_solo_llevan_la_meteo_observada():
    cal = construir_calendario(
        T4_2026,
        festivos(),
        eventos(),
        meteo((date(2026, 12, 1), "MAD", 6.0, 4.0), (date(2025, 12, 1), "BCN", 12.0, 0.0)),
    )

    por_fecha = {linea.split(" | ")[0]: linea for linea in lineas_prompt(cal, climatologia=False)}

    assert por_fecha["2026-12-01"].endswith("| eventos: sin datos | meteo: MAD 6°C 4.0mm (obs.)")  # BCN: solo clim.
    assert por_fecha["2026-12-02"] == "2026-12-02 | mié | festivo: no | eventos: sin datos"  # las dos, clim.
    assert por_fecha["2026-10-05"] == "2026-10-05 | lun | festivo: no | eventos: sin datos"  # sin ningún dato


def test_con_contexto_cada_linea_dice_si_es_vispera_puente_regreso_o_junto_a_un_evento():
    cal = construir_calendario(
        T4_2026,
        festivos((date(2026, 12, 8), "Inmaculada Concepción")),  # martes: el lunes 7 es puente
        eventos((date(2026, 11, 18), "Partido", "MAD")),
        meteo((date(2026, 12, 1), "MAD", 6.0, 4.0)),
    )

    por_fecha = {linea.split(" | ")[0]: linea for linea in lineas_prompt(cal, climatologia=False, contexto=True)}

    assert por_fecha["2026-12-04"].endswith("| contexto: víspera de tramo festivo")
    assert por_fecha["2026-12-07"].endswith("| contexto: puente")
    assert por_fecha["2026-12-08"] == (
        "2026-12-08 | mar | festivo: Inmaculada Concepción | eventos: ninguno | contexto: regreso de tramo festivo"
    )
    assert por_fecha["2026-11-17"].endswith("| contexto: junto a un evento")
    assert por_fecha["2026-11-18"].endswith("| eventos: Partido (MAD) | contexto: ninguno")
    assert por_fecha["2026-12-01"].endswith("| contexto: ninguno | meteo: MAD 6°C 4.0mm (obs.)")  # la meteo, al final
    assert all("contexto:" not in linea for linea in lineas_prompt(cal))  # sin contexto, las líneas de siempre


def _marcados(tipos, columna):
    return set(tipos.loc[tipos[columna], "fecha"])


def test_tipos_de_dia_con_los_festivos_nacionales_de_2026_T4():
    cal = construir_calendario(
        T4_2026,
        festivos(
            (date(2026, 10, 12), "Fiesta Nacional de España"),  # lunes
            (date(2026, 11, 1), "Todos los Santos"),  # domingo
            (date(2026, 12, 6), "Día de la Constitución"),  # domingo
            (date(2026, 12, 8), "Inmaculada Concepción"),  # martes
            (date(2026, 12, 25), "Navidad"),  # viernes
        ),
        eventos(),
        meteo(),
    )

    tipos = tipos_de_dia(cal)

    assert _marcados(tipos, "puente") == {date(2026, 12, 7)}  # lunes entre el domingo 6 y el martes 8
    assert _marcados(tipos, "vispera") == {date(2026, 10, 9), date(2026, 10, 30), date(2026, 12, 4), date(2026, 12, 24)}
    assert _marcados(tipos, "regreso") == {date(2026, 10, 12), date(2026, 11, 1), date(2026, 12, 8), date(2026, 12, 27)}
    assert not tipos["junto_a_evento"].any()
    assert list(tipos["fecha"]) == list(cal.dias["fecha"])  # conserva el calendario y solo añade columnas


def test_un_festivo_suelto_entre_semana_tiene_vispera_pero_no_regreso_ni_puente():
    cal = construir_calendario(T4_2026, festivos((date(2026, 11, 18), "Festivo local")), eventos(), meteo())  # miércoles

    tipos = tipos_de_dia(cal)

    assert _marcados(tipos, "vispera") == {date(2026, 11, 17)}
    assert not tipos["regreso"].any() and not tipos["puente"].any()


def test_en_los_bordes_del_trimestre_no_se_marca_lo_que_depende_de_un_dia_de_fuera():
    # 2026-T1 empieza en un tramo festivo: jue 1 Año Nuevo, vie 2 puente, fin de semana, lun 5 puente, mar 6 Reyes.
    inicio = tipos_de_dia(
        construir_calendario(
            Trimestre(2026, 1), festivos((date(2026, 1, 1), "Año Nuevo"), (date(2026, 1, 6), "Reyes")), eventos(), meteo()
        )
    )
    # 2028-T4 acaba en un tramo festivo (sáb 30 festivo, dom 31): no se sabe si sigue en enero.
    fin = tipos_de_dia(construir_calendario(Trimestre(2028, 4), festivos((date(2028, 12, 30), "Festivo")), eventos(), meteo()))

    assert _marcados(inicio, "puente") == {date(2026, 1, 2), date(2026, 1, 5)}
    assert not inicio["vispera"].any()  # sería el 31 de diciembre, de otro trimestre
    assert _marcados(inicio, "regreso") == {date(2026, 1, 6)}
    assert _marcados(fin, "vispera") == {date(2028, 12, 29)}
    assert not fin["regreso"].any()


def test_junto_a_un_evento_son_el_dia_anterior_y_el_siguiente_sin_evento_propio():
    cal = construir_calendario(
        T4_2026,
        festivos(),
        eventos(
            (date(2026, 10, 1), "Feria", "MAD"),  # primer día del trimestre: solo cuenta el siguiente
            (date(2026, 11, 10), "Partido", "MAD"),
            (date(2026, 11, 11), "Concierto", "BCN"),  # seguidos: ninguno de los dos está «junto a» otro
        ),
        meteo(),
    )

    assert _marcados(tipos_de_dia(cal), "junto_a_evento") == {date(2026, 10, 2), date(2026, 11, 9), date(2026, 11, 12)}
