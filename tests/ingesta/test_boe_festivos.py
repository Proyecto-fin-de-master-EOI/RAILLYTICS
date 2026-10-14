"""Tests del parseo del calendario laboral del BOE.

Sin red: se construye un XML con la misma forma que el del BOE (meses alternados con festivos, y
una columna por comunidad marcada con asteriscos). Lo delicado es la alineación de columnas: si se
desplaza una, se cuentan los festivos de otra comunidad y la serie sale creíble pero falsa.
"""
from datetime import date

import pytest

from raillytics.ingesta.boe_festivos import parsear

# Solo tres comunidades: basta para probar la alineación, y una de ellas (Andalucía) debe quedar
# siempre fuera porque no es del corredor.
CABECERA = ["Andalucía", "Cataluña (2)", "Com. Madrid"]


def _xml(filas: list[list[str]], anio: int = 2026, titulo: str | None = None) -> bytes:
    """Un documento con la forma del BOE: <documento> con metadatos y la tabla dentro de <texto>."""
    if titulo is None:
        titulo = f"Resolución de 17 de octubre, por la que se publican las fiestas laborales para el año {anio}"
    def tr(celdas: list[str]) -> str:
        return "<tr>" + "".join(f"<td>{c}</td>" for c in celdas) + "</tr>"
    cuerpo = tr(["Fecha de las fiestas", "Comunidades Autónomas"]) + tr(CABECERA)
    cuerpo += "".join(tr(f) for f in filas)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f"<documento><metadatos><titulo>{titulo}</titulo></metadatos>"
        f"<texto><table><tbody>{cuerpo}</tbody></table></texto></documento>"
    ).encode("utf-8")


def _mes(nombre: str) -> list[str]:
    return [nombre, "", "", ""]


def test_un_festivo_de_los_dos_extremos_no_lleva_sufijo():
    xml = _xml([_mes("Enero"), ["1 Año Nuevo.", "*", "*", "*"]])

    assert parsear(xml, 2026) == [{"fecha": date(2026, 1, 1), "nombre": "Año Nuevo"}]


def test_un_festivo_de_un_solo_extremo_lo_dice_en_el_nombre():
    xml = _xml([
        _mes("Mayo"),
        ["2 Fiesta de la Comunidad de Madrid.", "", "", "***"],
        _mes("Septiembre"),
        ["11 Fiesta Nacional de Cataluña.", "", "***", ""],
    ])

    assert parsear(xml, 2026) == [
        {"fecha": date(2026, 5, 2), "nombre": "Fiesta de la Comunidad de Madrid (Madrid)"},
        {"fecha": date(2026, 9, 11), "nombre": "Fiesta Nacional de Cataluña (Cataluña)"},
    ]


def test_el_ambito_lo_marcan_las_dos_comunidades_no_el_tipo_de_asterisco():
    # El Jueves Santo es fiesta NACIONAL (y lleva marca de nacional), pero en Cataluña se trabaja.
    # Para este corredor es un festivo de un solo extremo, y el nombre tiene que decirlo.
    xml = _xml([_mes("Abril"), ["2 Jueves Santo.", "**", "", "**"]])

    assert parsear(xml, 2026) == [{"fecha": date(2026, 4, 2), "nombre": "Jueves Santo (Madrid)"}]


def test_se_salta_un_festivo_que_no_es_de_ninguno_de_los_dos_extremos():
    xml = _xml([_mes("Febrero"), ["28 Día de Andalucía.", "***", "", ""]])

    assert parsear(xml, 2026) == []


def test_el_mes_se_arrastra_de_la_fila_anterior():
    # Las filas de festivo solo traen el día: el mes está en la fila de cabecera de mes.
    xml = _xml([
        _mes("Junio"),
        ["24 San Juan.", "", "***", ""],
        _mes("Diciembre"),
        ["26 San Esteban.", "", "***", ""],
    ])

    assert [f["fecha"] for f in parsear(xml, 2026)] == [date(2026, 6, 24), date(2026, 12, 26)]


def test_una_resolucion_de_otro_anio_se_rechaza():
    # Red de seguridad contra apuntar una fuente del registro al BOE equivocado.
    xml = _xml([_mes("Enero"), ["1 Año Nuevo.", "*", "*", "*"]], anio=2025)

    with pytest.raises(ValueError, match="no menciona el año 2026"):
        parsear(xml, 2026)


def test_si_falta_una_comunidad_del_corredor_falla_en_vez_de_contar_otra():
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<documento><metadatos><titulo>fiestas laborales para el año 2026</titulo></metadatos>"
        "<texto><table><tbody>"
        "<tr><td>Fecha de las fiestas</td><td>Comunidades Autónomas</td></tr>"
        "<tr><td>Andalucía</td><td>Aragón</td><td>Com. Madrid</td></tr>"
        "<tr><td>Enero</td><td></td><td></td><td></td></tr>"
        "</tbody></table></texto></documento>"
    ).encode("utf-8")

    with pytest.raises(ValueError, match="Cataluña"):
        parsear(xml, 2026)


def test_un_documento_sin_tabla_se_rechaza():
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<documento><metadatos><titulo>fiestas laborales para el año 2026</titulo></metadatos>"
        "<texto><p>Sin tabla.</p></texto></documento>"
    ).encode("utf-8")

    with pytest.raises(ValueError, match="tabla de festivos"):
        parsear(xml, 2026)
