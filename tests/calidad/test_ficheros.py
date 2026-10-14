import io
import zipfile

import pytest

from raillytics.calidad.ficheros import motivo_rechazo, validar_contenido

def _zip(*miembros: tuple[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for nombre, contenido in miembros:
            zf.writestr(nombre, contenido)
    return buf.getvalue()


def _por_gate(resultados):
    return {r.gate: r for r in resultados}


def test_csv_valido_pasa_todos_los_gates():
    gates = _por_gate(validar_contenido(b"estacion,viajeros\nAtocha,100\n", "csv", "text/csv; charset=utf-8", tabla="crtm"))

    assert [g.resultado for g in gates.values()] == ["ok", "ok", "ok"]
    assert gates["contenido_no_vacio"].valor == 29.0
    assert all(g.tabla == "crtm" for g in gates.values())
    assert motivo_rechazo(gates.values()) is None


def test_zip_declarado_como_csv_se_rechaza():
    """El caso real de CRTM: el servidor devuelve un GTFS (zip) y el YAML decía csv."""
    contenido = _zip(("stops.txt", b"stop_id,stop_name\n1,Atocha\n"))

    gates = _por_gate(validar_contenido(contenido, "csv", "application/zip"))

    assert gates["formato_declarado"].resultado == "fallo"
    assert gates["formato_declarado"].severidad == "bloqueante"
    assert "parece ZIP" in gates["formato_declarado"].detalle
    assert gates["content_type"].resultado == "fallo"
    assert motivo_rechazo(gates.values()) == f"formato_declarado: {gates['formato_declarado'].detalle}"


def test_zip_valido_declarado_como_zip_pasa():
    gates = _por_gate(validar_contenido(_zip(("stops.txt", b"a,b\n1,2\n")), "zip", "application/zip"))

    assert [g.resultado for g in gates.values()] == ["ok", "ok", "ok"]


@pytest.mark.parametrize(
    ("contenido", "formato", "fragmento"),
    [
        (b"<html><body>Zscaler</body></html>", "json", "parece HTML"),
        # Una página de error de proxy es XML bien formado: sin distinguirla del XML de verdad,
        # colaría como contenido válido en una fuente xml.
        (b"<html><body>Zscaler</body></html>", "xml", "parece HTML"),
        (b'<?xml version="1.0"?><documento><texto>', "xml", "XML inválido"),
        (b'<?xml version="1.0"?><documento/>', "xml", "vacío"),
        (_zip(("a.txt", b"x")), "xml", "parece ZIP"),
        (b'{"entity": [', "json", "JSON inválido"),
        (b"solo una columna\n1\n2\n", "csv", "no tiene delimitador"),
        (b'{"a": 1}', "csv", "parece JSON"),
        (b"no soy un zip", "zip", "parece texto"),
        (_zip(), "zip", "no contiene"),
    ],
)
def test_contenido_que_no_encaja_con_el_formato_se_rechaza(contenido, formato, fragmento):
    gates = _por_gate(validar_contenido(contenido, formato))

    assert gates["formato_declarado"].resultado == "fallo"
    assert fragmento in gates["formato_declarado"].detalle
    assert motivo_rechazo(gates.values()) is not None


def test_contenido_vacio_se_rechaza_sin_evaluar_el_formato():
    resultados = validar_contenido(b"", "json", "application/json")

    assert [r.gate for r in resultados] == ["contenido_no_vacio", "content_type"]
    assert resultados[0].resultado == "fallo" and resultados[0].bloquea
    assert motivo_rechazo(resultados) == "contenido_no_vacio: el servidor devolvió 0 bytes"


def test_content_type_incoherente_solo_avisa():
    gates = _por_gate(validar_contenido(b'{"a": 1}', "json", "text/html"))

    assert gates["content_type"].resultado == "fallo"
    assert gates["content_type"].severidad == "aviso"
    assert not gates["content_type"].bloquea
    assert motivo_rechazo(gates.values()) is None   # un aviso no rechaza el fichero


CNMC = "﻿Trimestre;Corredor;Viajeros (Núm)\n2026T2;Madrid-Barcelona;100\n2026T1;Madrid-Barcelona;90\n".encode("utf-8")
COLUMNAS = ["Trimestre", "Corredor", "Viajeros (Núm)"]


def test_un_csv_con_delimitador_bom_y_checks_declarados_pasa_todos_los_gates():
    gates = _por_gate(
        validar_contenido(
            CNMC, "csv", "text/csv", tabla="cnmc",
            opciones={"delimiter": ";"}, checks={"min_bytes": 20, "min_filas": 2, "columnas": COLUMNAS},
        )
    )

    assert list(gates) == ["contenido_no_vacio", "formato_declarado", "content_type", "tamano_minimo", "cabecera_esperada", "filas_minimas"]
    assert all(g.resultado == "ok" for g in gates.values())
    assert gates["tamano_minimo"].valor == float(len(CNMC)) and gates["filas_minimas"].valor == 2.0
    assert motivo_rechazo(gates.values()) is None


def test_una_cabecera_distinta_de_la_esperada_se_rechaza_con_las_dos_cabeceras_en_el_motivo():
    cambiada = CNMC.replace("Corredor".encode(), "Trayecto".encode())

    gates = _por_gate(validar_contenido(cambiada, "csv", opciones={"delimiter": ";"}, checks={"columnas": COLUMNAS}))

    assert gates["cabecera_esperada"].resultado == "fallo" and gates["cabecera_esperada"].severidad == "bloqueante"
    assert "Trayecto" in gates["cabecera_esperada"].detalle and "Corredor" in gates["cabecera_esperada"].detalle
    assert motivo_rechazo(gates.values()).startswith("cabecera_esperada:")


def test_pocas_filas_o_pocos_bytes_se_rechazan():
    gates = _por_gate(validar_contenido(CNMC, "csv", opciones={"delimiter": ";"}, checks={"min_bytes": 5000, "min_filas": 50}))

    assert gates["tamano_minimo"].resultado == "fallo" and gates["tamano_minimo"].umbral == ">= 5000"
    assert gates["filas_minimas"].resultado == "fallo" and gates["filas_minimas"].valor == 2.0


def test_el_delimitador_declarado_sustituye_a_la_deteccion_automatica():
    # Con `;` en la cabecera, declarar `,` es un error de configuración que debe verse, no pasar por «tiene algún delimitador».
    gates = _por_gate(validar_contenido(CNMC, "csv", opciones={"delimiter": ","}))

    assert gates["formato_declarado"].resultado == "fallo"
    assert "no tiene delimitador" in gates["formato_declarado"].detalle


def test_con_la_codificacion_declarada_se_comprueba_la_cabecera_con_tildes():
    latin = "Trimestre;Viajeros (Núm)\n2026T2;100\n".encode("latin-1")

    gates = _por_gate(
        validar_contenido(latin, "csv", opciones={"delimiter": ";", "encoding": "latin-1"}, checks={"columnas": ["Trimestre", "Viajeros (Núm)"]})
    )

    assert gates["cabecera_esperada"].resultado == "ok"


def test_si_el_formato_no_encaja_no_se_evaluan_los_checks_del_csv():
    gates = _por_gate(validar_contenido(b"<html>mantenimiento</html>", "csv", opciones={"delimiter": ";"}, checks={"min_filas": 10, "columnas": COLUMNAS}))

    assert "cabecera_esperada" not in gates and "filas_minimas" not in gates
    assert gates["formato_declarado"].resultado == "fallo"


def test_xml_valido_declarado_como_xml_pasa():
    # La forma del XML del BOE: declaración, un documento raíz y la tabla de festivos dentro.
    contenido = (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b"<documento><texto><table><tbody><tr><td>1 de enero</td></tr></tbody></table></texto></documento>"
    )

    gates = _por_gate(validar_contenido(contenido, "xml", "application/xml; charset=utf-8"))

    assert [g.resultado for g in gates.values()] == ["ok", "ok", "ok"]


def test_un_xml_con_entidades_declaradas_se_rechaza_en_vez_de_expandirse():
    # «Billion laughs»: con el parser de la librería estándar estas entidades se expanden hasta
    # agotar la memoria del proceso. defusedxml las rechaza y el gate lo deja en un fichero a
    # cuarentena, que es lo que debe pasar con un XML que llega de internet.
    bomba = (
        b'<?xml version="1.0"?>'
        b'<!DOCTYPE lolz [<!ENTITY lol "lol"><!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;">]>'
        b"<lolz>&lol2;</lolz>"
    )

    gates = _por_gate(validar_contenido(bomba, "xml"))

    assert gates["formato_declarado"].resultado == "fallo"
    assert "XML inválido" in gates["formato_declarado"].detalle
