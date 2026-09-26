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
        (b"<html><body>Zscaler</body></html>", "json", "parece HTML/XML"),
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
