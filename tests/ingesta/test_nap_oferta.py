"""Tests de la serie diaria de oferta.

Lo que se prueba aquí es sobre todo lo que puede salir mal **en silencio**: un snapshot que no
cubre el día, uno corrupto que contaría 0 trenes sin dar error, y los ceros que son artefacto del
cambio de horario. Un fallo en cualquiera de los tres da una serie creíble pero falsa.
"""
from __future__ import annotations

import zipfile
from datetime import date
from pathlib import Path

from raillytics.ingesta.nap_oferta import (
    _limpiar_ceros_espurios,
    coherente,
    construir,
    elegir_snapshots,
    trenes_por_dias,
    vigencia,
)

MADRID = "60000"
BARCELONA = "71801"
TODOS_LOS_DIAS = {d: "1" for d in
                  ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")}


def _csv(cabecera: list[str], filas: list[dict]) -> str:
    lineas = [",".join(cabecera)]
    lineas += [",".join(str(f.get(c, "")) for c in cabecera) for f in filas]
    return "\n".join(lineas) + "\n"


def _gtfs(
    destino: Path,
    *,
    trips: list[dict],
    paradas: dict[str, list[str]],
    rutas: list[dict],
    calendario: list[dict],
    excepciones: list[dict] | None = None,
) -> Path:
    """Escribe un GTFS mínimo pero válido: lo justo que mira `trenes_por_dias`."""
    stop_times = [
        {"trip_id": trip_id, "stop_id": stop_id, "stop_sequence": i}
        for trip_id, stops in paradas.items()
        for i, stop_id in enumerate(stops)
    ]
    with zipfile.ZipFile(destino, "w") as zf:
        zf.writestr("trips.txt", _csv(["trip_id", "service_id", "route_id"], trips))
        zf.writestr("stop_times.txt", _csv(["trip_id", "stop_id", "stop_sequence"], stop_times))
        zf.writestr("routes.txt", _csv(["route_id", "route_short_name"], rutas))
        zf.writestr(
            "calendar.txt",
            _csv(["service_id", "start_date", "end_date", *TODOS_LOS_DIAS], calendario),
        )
        if excepciones is not None:
            zf.writestr("calendar_dates.txt", _csv(["service_id", "date", "exception_type"], excepciones))
    return destino


def _gtfs_simple(destino: Path, *, inicio: str, fin: str, stop_barcelona: str = BARCELONA) -> Path:
    """Un GTFS con dos trenes Madrid-Barcelona: uno AVE y otro ALVIA."""
    return _gtfs(
        destino,
        trips=[
            {"trip_id": "t_ave", "service_id": "s1", "route_id": "r_ave"},
            {"trip_id": "t_alvia", "service_id": "s1", "route_id": "r_alvia"},
        ],
        paradas={"t_ave": [MADRID, stop_barcelona], "t_alvia": [MADRID, stop_barcelona]},
        rutas=[{"route_id": "r_ave", "route_short_name": "AVE"},
               {"route_id": "r_alvia", "route_short_name": "ALVIA"}],
        calendario=[{"service_id": "s1", "start_date": inicio, "end_date": fin, **TODOS_LOS_DIAS}],
    )


# --- conteo de trenes ---


def test_cuenta_alta_velocidad_aparte_del_total(tmp_path):
    z = _gtfs_simple(tmp_path / "2026-01-01_1.zip", inicio="20260101", fin="20261231")

    conteo = trenes_por_dias(z, [date(2026, 3, 10)])[date(2026, 3, 10)]["Madrid-Barcelona"]

    # ALVIA circula por vía convencional y CNMC lo clasifica fuera de «LD AV»: cuenta en el total
    # pero no en alta velocidad, que es la cifra comparable con la demanda de CNMC.
    assert conteo == {"alta_velocidad": 1, "todos": 2}


def test_no_cuenta_un_tren_que_no_toca_las_dos_cabeceras(tmp_path):
    z = _gtfs(
        tmp_path / "2026-01-01_1.zip",
        trips=[{"trip_id": "t", "service_id": "s1", "route_id": "r"}],
        paradas={"t": [MADRID, "51003"]},  # Madrid-Sevilla, no pasa por Barcelona
        rutas=[{"route_id": "r", "route_short_name": "AVE"}],
        calendario=[{"service_id": "s1", "start_date": "20260101", "end_date": "20261231", **TODOS_LOS_DIAS}],
    )

    conteo = trenes_por_dias(z, [date(2026, 3, 10)])[date(2026, 3, 10)]

    assert conteo["Madrid-Barcelona"] == {"alta_velocidad": 0, "todos": 0}
    assert conteo["Madrid-Sevilla"] == {"alta_velocidad": 1, "todos": 1}


def test_normaliza_el_prefijo_uic_que_usa_ouigo(tmp_path):
    # OUIGO publica los mismos códigos de estación con el prefijo UIC de España: 007171801 = 71801.
    # Sin normalizar, toda la oferta de OUIGO saldría a cero.
    z = _gtfs_simple(tmp_path / "2026-01-01_1.zip", inicio="20260101", fin="20261231",
                     stop_barcelona="007171801")

    conteo = trenes_por_dias(z, [date(2026, 3, 10)])[date(2026, 3, 10)]["Madrid-Barcelona"]

    assert conteo["alta_velocidad"] == 1


def test_no_cuenta_un_dia_fuera_del_calendario_del_servicio(tmp_path):
    z = _gtfs_simple(tmp_path / "2026-01-01_1.zip", inicio="20260101", fin="20260131")

    conteo = trenes_por_dias(z, [date(2026, 3, 10)])[date(2026, 3, 10)]["Madrid-Barcelona"]

    assert conteo == {"alta_velocidad": 0, "todos": 0}


def test_una_excepcion_de_calendar_dates_manda_sobre_el_calendario(tmp_path):
    z = _gtfs(
        tmp_path / "2026-01-01_1.zip",
        trips=[{"trip_id": "t", "service_id": "s1", "route_id": "r"}],
        paradas={"t": [MADRID, BARCELONA]},
        rutas=[{"route_id": "r", "route_short_name": "AVE"}],
        calendario=[{"service_id": "s1", "start_date": "20260101", "end_date": "20261231", **TODOS_LOS_DIAS}],
        # tipo 2 = ese día NO circula, aunque el calendario semanal diga que sí.
        excepciones=[{"service_id": "s1", "date": "20260310", "exception_type": "2"}],
    )

    conteos = trenes_por_dias(z, [date(2026, 3, 10), date(2026, 3, 11)])

    assert conteos[date(2026, 3, 10)]["Madrid-Barcelona"]["alta_velocidad"] == 0
    assert conteos[date(2026, 3, 11)]["Madrid-Barcelona"]["alta_velocidad"] == 1


def test_cuenta_cada_dia_pedido_con_una_sola_pasada(tmp_path):
    # Un snapshot de OUIGO cubre semanas: debe devolver una entrada por día sin reparsear el ZIP.
    z = _gtfs(
        tmp_path / "2026-01-01_1.zip",
        trips=[{"trip_id": "t", "service_id": "s_laborables", "route_id": "r"}],
        paradas={"t": [MADRID, BARCELONA]},
        rutas=[{"route_id": "r", "route_short_name": "AVE"}],
        calendario=[{
            "service_id": "s_laborables", "start_date": "20260101", "end_date": "20261231",
            **TODOS_LOS_DIAS, "saturday": "0", "sunday": "0",
        }],
    )

    dias = [date(2026, 3, 13), date(2026, 3, 14), date(2026, 3, 15)]  # viernes, sábado, domingo
    conteos = trenes_por_dias(z, dias)

    assert [conteos[d]["Madrid-Barcelona"]["alta_velocidad"] for d in dias] == [1, 0, 0]


# --- snapshots corruptos ---


def test_un_snapshot_con_trip_id_que_no_casa_se_detecta_como_corrupto(tmp_path):
    # Visto de verdad en renfe/2026-03-11: stop_times usa "0043712026-10-07" y trips "3282V23340C1".
    # Sin detectarlo el conteo sale 0 y en el dashboard se leería como que no circuló ningún tren.
    z = _gtfs(
        tmp_path / "2026-03-11_1.zip",
        trips=[{"trip_id": "3282V23340C1", "service_id": "s1", "route_id": "r"}],
        paradas={"0043712026-10-07": [MADRID, BARCELONA]},
        rutas=[{"route_id": "r", "route_short_name": "AVE"}],
        calendario=[{"service_id": "s1", "start_date": "20260101", "end_date": "20261231", **TODOS_LOS_DIAS}],
    )

    assert coherente(z) is False
    assert trenes_por_dias(z, [date(2026, 3, 11)]) is None


def test_un_snapshot_sin_los_ficheros_minimos_no_sirve(tmp_path):
    z = tmp_path / "2026-01-01_1.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("agency.txt", "agency_id\nrenfe\n")

    assert coherente(z) is False
    assert trenes_por_dias(z, [date(2026, 1, 1)]) is None
    assert vigencia(z) is None


def test_un_zip_ilegible_no_tira_el_proceso(tmp_path):
    z = tmp_path / "2026-01-01_1.zip"
    z.write_bytes(b"esto no es un zip")

    assert coherente(z) is False
    assert vigencia(z) is None


# --- elección del snapshot vigente ---


def test_elige_el_snapshot_vigente_mas_reciente_no_el_publicado_ese_dia(tmp_path):
    # El caso de OUIGO: publica en septiembre un horario que empieza en diciembre. Usar «el
    # snapshot del día» daría 0 trenes en octubre, que es falso: vale el de junio, que sigue vigente.
    _gtfs_simple(tmp_path / "2025-06-10_1.zip", inicio="20250610", fin="20251130")
    _gtfs_simple(tmp_path / "2025-09-20_2.zip", inicio="20251201", fin="20260331")

    elegido = elegir_snapshots(tmp_path.glob("*.zip"), date(2025, 10, 15), date(2025, 10, 15))

    assert elegido[date(2025, 10, 15)].stem == "2025-06-10_1"


def test_un_dia_que_ningun_snapshot_cubre_se_queda_sin_dato(tmp_path):
    # Sin dato no es lo mismo que cero trenes: es lo que pasa con OUIGO desde que dejó de publicar.
    _gtfs_simple(tmp_path / "2025-06-10_1.zip", inicio="20250610", fin="20250630")

    elegido = elegir_snapshots(tmp_path.glob("*.zip"), date(2025, 8, 1), date(2025, 8, 3))

    assert elegido == {}


def test_si_el_snapshot_mas_reciente_esta_corrupto_se_usa_el_anterior(tmp_path):
    _gtfs_simple(tmp_path / "2026-03-01_1.zip", inicio="20260301", fin="20261231")
    _gtfs(
        tmp_path / "2026-03-11_2.zip",
        trips=[{"trip_id": "no_casa", "service_id": "s1", "route_id": "r"}],
        paradas={"otro_esquema": [MADRID, BARCELONA]},
        rutas=[{"route_id": "r", "route_short_name": "AVE"}],
        calendario=[{"service_id": "s1", "start_date": "20260301", "end_date": "20261231", **TODOS_LOS_DIAS}],
    )

    elegido = elegir_snapshots(tmp_path.glob("*.zip"), date(2026, 3, 12), date(2026, 3, 12))

    assert elegido[date(2026, 3, 12)].stem == "2026-03-01_1"


# --- ceros espurios ---


def _registro(fecha: date, trenes: int) -> dict:
    return {"operador": "renfe", "corredor": "Madrid-Barcelona", "fecha": fecha,
            "trenes": trenes, "trenes_todos": trenes, "snapshot": "x"}


def test_descarta_el_cero_aislado_entre_dias_normales(tmp_path):
    # Artefacto del cambio de horario: el GTFS cubre la fecha, pero el corredor empieza al día
    # siguiente. Se descarta el registro en vez de inventar un valor interpolado.
    registros = [_registro(date(2026, 3, d), 0 if d == 14 else 30) for d in range(11, 18)]

    limpios = _limpiar_ceros_espurios(registros)

    assert date(2026, 3, 14) not in {r["fecha"] for r in limpios}
    assert len(limpios) == 6


def test_conserva_una_racha_larga_de_ceros(tmp_path):
    # Un cero de verdad: el operador no presta ese servicio. No se toca.
    registros = [_registro(date(2026, 3, d), 0) for d in range(11, 25)]

    limpios = _limpiar_ceros_espurios(registros)

    assert len(limpios) == len(registros)


def test_un_cero_con_el_total_a_uno_sigue_siendo_espurio(tmp_path):
    # En un cambio de horario puede quedar algún regional suelto: `trenes_todos` vale 1 mientras la
    # alta velocidad está a 0. Se mira `trenes`, que es la métrica que se usa.
    registros = [_registro(date(2026, 3, d), 30) for d in range(11, 18)]
    medio = next(r for r in registros if r["fecha"] == date(2026, 3, 14))
    medio["trenes"], medio["trenes_todos"] = 0, 1

    limpios = _limpiar_ceros_espurios(registros)

    assert date(2026, 3, 14) not in {r["fecha"] for r in limpios}


# --- qué tren es del corredor ---


SEVILLA = "51003"


def test_no_cuenta_un_tren_que_solo_pasa_por_madrid_camino_de_otro_sitio(tmp_path):
    # El AVE Sevilla-Barcelona para en Madrid de paso. CNMC lo clasifica en otro corredor, y
    # contarlo aquí inflaba Renfe entre un 11 % y un 22 % contra el Tren.km de CNMC.
    z = _gtfs(
        tmp_path / "2026-01-01_1.zip",
        trips=[{"trip_id": "t", "service_id": "s1", "route_id": "r"}],
        paradas={"t": [SEVILLA, MADRID, BARCELONA]},   # empieza en Sevilla, no en Madrid
        rutas=[{"route_id": "r", "route_short_name": "AVE"}],
        calendario=[{"service_id": "s1", "start_date": "20260101", "end_date": "20261231", **TODOS_LOS_DIAS}],
    )

    conteo = trenes_por_dias(z, [date(2026, 3, 10)])[date(2026, 3, 10)]

    assert conteo["Madrid-Barcelona"] == {"alta_velocidad": 0, "todos": 0}
    assert conteo["Madrid-Sevilla"] == {"alta_velocidad": 0, "todos": 0}


def test_cuenta_un_tren_que_sale_de_madrid_y_sigue_mas_alla_de_barcelona(tmp_path):
    # El Madrid-Figueres sirve el corredor aunque no termine en Barcelona.
    z = _gtfs(
        tmp_path / "2026-01-01_1.zip",
        trips=[{"trip_id": "t", "service_id": "s1", "route_id": "r"}],
        paradas={"t": [MADRID, BARCELONA, "71802"]},
        rutas=[{"route_id": "r", "route_short_name": "AVE"}],
        calendario=[{"service_id": "s1", "start_date": "20260101", "end_date": "20261231", **TODOS_LOS_DIAS}],
    )

    conteo = trenes_por_dias(z, [date(2026, 3, 10)])[date(2026, 3, 10)]["Madrid-Barcelona"]

    assert conteo["alta_velocidad"] == 1


def test_cuenta_el_tren_en_los_dos_sentidos(tmp_path):
    # Madrid-Barcelona y Barcelona-Madrid son dos trenes distintos y los dos son del corredor.
    z = _gtfs(
        tmp_path / "2026-01-01_1.zip",
        trips=[{"trip_id": "ida", "service_id": "s1", "route_id": "r"},
               {"trip_id": "vuelta", "service_id": "s1", "route_id": "r"}],
        paradas={"ida": [MADRID, BARCELONA], "vuelta": [BARCELONA, MADRID]},
        rutas=[{"route_id": "r", "route_short_name": "AVE"}],
        calendario=[{"service_id": "s1", "start_date": "20260101", "end_date": "20261231", **TODOS_LOS_DIAS}],
    )

    conteo = trenes_por_dias(z, [date(2026, 3, 10)])[date(2026, 3, 10)]["Madrid-Barcelona"]

    assert conteo["alta_velocidad"] == 2


# --- cobertura: días a los que les falta un operador ---


def test_marca_los_dias_a_los_que_les_falta_un_operador(tmp_path):
    # Si un operador no publicó ese día, el total NO es menos servicio: es dato incompleto.
    for operador, dias in (("renfe", ("20260310", "20260311")), ("ouigo", ("20260310", "20260310"))):
        carpeta = tmp_path / operador
        carpeta.mkdir()
        _gtfs(
            carpeta / "2026-03-10_1.zip",
            trips=[{"trip_id": f"t_{operador}", "service_id": "s1", "route_id": "r"}],
            paradas={f"t_{operador}": [MADRID, BARCELONA]},
            rutas=[{"route_id": "r", "route_short_name": "AVE"}],
            calendario=[{"service_id": "s1", "start_date": dias[0], "end_date": dias[1], **TODOS_LOS_DIAS}],
        )

    df = construir(tmp_path, date(2026, 3, 10), date(2026, 3, 11))
    mb = df[df["corredor"] == "Madrid-Barcelona"]

    # El 10 lo publican los dos; el 11 solo renfe, porque el calendario de ouigo no llega.
    assert set(mb[mb["fecha"] == date(2026, 3, 10)]["cobertura_completa"]) == {True}
    assert set(mb[mb["fecha"] == date(2026, 3, 11)]["cobertura_completa"]) == {False}
