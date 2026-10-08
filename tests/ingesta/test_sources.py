import textwrap
from pathlib import Path

import pytest
import yaml

from raillytics.ingesta.sources import DataSource, SilverTabla, load_sources

FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_load_sources_parses_valid_yaml():
    sources = load_sources(FIXTURES / "data_sources_sample.yml")

    assert sources == [
        DataSource(
            id="crtm",
            name="CRTM - Consorcio Regional de Transportes de Madrid",
            url="https://crtm.maps.arcgis.com/sharing/rest/content/items/1a25440bf66f499bae2657ec7fb40144/data",
            format="csv",
        )
    ]


def test_load_sources_accepts_zip_and_the_project_registry_is_consistent():
    # El registro real del proyecto: CRTM sirve un GTFS (zip); Renfe, JSON; la CNMC, tres CSV con `;`;
    # AEMET, JSON con descarga en dos pasos; el NAP, el GTFS de cada operador en zip; el BOE, el
    # calendario laboral de cada año en xml.
    fuentes = load_sources(Path(__file__).parents[2] / "config" / "data_sources.yml")
    sources = {s.id: s.format for s in fuentes}

    assert sources == {
        "crtm": "zip", "renfe_trip_updates": "json", "renfe_vehicle_positions": "json",
        "cnmc_indicadores": "csv", "cnmc_precio_trimestral": "csv", "cnmc_precio_mensual": "csv",
        "aemet_madrid_barajas": "json", "aemet_barcelona_prat": "json",
        "nap_gtfs_renfe": "zip", "nap_gtfs_ouigo": "zip",
        "boe_calendario_laboral_2025": "xml", "boe_calendario_laboral_2026": "xml",
    }
    # Las fuentes con credencial declaran de qué variable de entorno sale, nunca el secreto.
    con_auth = {s.id: s.auth["env"] for s in fuentes if s.auth}
    assert con_auth == {
        "aemet_madrid_barajas": "AEMET_API_KEY", "aemet_barcelona_prat": "AEMET_API_KEY",
        "nap_gtfs_renfe": "NAP_API_KEY", "nap_gtfs_ouigo": "NAP_API_KEY",
    }


def test_load_sources_rejects_unsupported_format(tmp_path):
    bad_yaml = tmp_path / "bad.yml"
    bad_yaml.write_text(
        "sources:\n"
        "  - id: bad\n"
        "    name: Bad source\n"
        "    url: https://example.invalid/data\n"
        "    format: xlsx\n"
    )

    with pytest.raises(ValueError, match="Formato no soportado"):
        load_sources(bad_yaml)


CNMC = textwrap.dedent(
    """\
    sources:
      - id: cnmc
        name: "CNMC prueba"
        url: https://example.invalid/ds.csv
        format: csv
        options: {delimiter: ";"}
        checks:
          min_bytes: 1000
          min_filas: 5
          columnas: ["Trimestre", "Viajeros (Núm)"]
        silver:
          - {tabla: cnmc_trimestral, modo: snapshot}
    """
)


def _yaml(tmp_path, texto):
    ruta = tmp_path / "fuentes.yml"
    ruta.write_text(texto, encoding="utf-8")
    return ruta


def test_una_fuente_con_opciones_checks_y_silver_se_parsea(tmp_path):
    (fuente,) = load_sources(_yaml(tmp_path, CNMC))

    assert fuente.options == {"delimiter": ";"}
    assert fuente.checks == {"min_bytes": 1000, "min_filas": 5, "columnas": ["Trimestre", "Viajeros (Núm)"]}
    assert fuente.silver == (SilverTabla(tabla="cnmc_trimestral", modo="snapshot"),)


def test_una_fuente_sin_extras_tiene_valores_vacios_por_defecto():
    fuente = load_sources(FIXTURES / "data_sources_sample.yml")[0]

    assert fuente.options == {} and fuente.checks == {} and fuente.silver == ()


@pytest.mark.parametrize(
    ("cambio", "fragmento"),
    [
        ("    optons: {delimiter: ';'}\n", "Clave desconocida en la fuente 'cnmc': optons"),
        ("    options: {separator: ';'}\n", "options"),
        ("    checks: {min_byte: 10}\n", "checks"),
        ("    options: {delimiter: ';;'}\n", "un único carácter"),
        ("    checks: {min_filas: 0}\n", "entero >= 1"),
        ("    checks: {columnas: []}\n", "lista no vacía"),
        ("    silver: [{tabla: Mal-Nombre, modo: snapshot}]\n", "nombre de tabla Silver inválido"),
        ("    silver: [{tabla: ok, modo: tiempo_real}]\n", "modo Silver no soportado"),
        ("    silver: [{tabla: ok}]\n", "'modo'"),
    ],
)
def test_los_valores_invalidos_se_rechazan_diciendo_que_falla(tmp_path, cambio, fragmento):
    texto = "sources:\n  - id: cnmc\n    name: x\n    url: https://example.invalid/a.csv\n    format: csv\n" + cambio

    with pytest.raises(ValueError, match=fragmento):
        load_sources(_yaml(tmp_path, texto))


def test_un_formato_que_no_admite_opciones_de_lectura_rechaza_options(tmp_path):
    texto = "sources:\n  - id: f\n    name: x\n    url: https://example.invalid/a.json\n    format: json\n    options: {delimiter: ';'}\n"

    with pytest.raises(ValueError, match="el formato 'json' no admite delimiter en 'options'"):
        load_sources(_yaml(tmp_path, texto))


def test_una_tabla_silver_no_puede_declararla_mas_de_una_fuente(tmp_path):
    una = "  - id: a\n    name: x\n    url: https://example.invalid/a.csv\n    format: csv\n    silver: [{tabla: t, modo: snapshot}]\n"
    otra = una.replace("id: a", "id: b")

    with pytest.raises(ValueError, match="más de una fuente: t"):
        load_sources(_yaml(tmp_path, "sources:\n" + una + otra))


def test_una_entrada_sin_un_campo_obligatorio_se_rechaza_nombrandolo(tmp_path):
    texto = "sources:\n  - ids: mal\n    name: x\n    url: https://example.invalid/a.csv\n    format: csv\n"

    with pytest.raises(ValueError, match="'id'"):
        load_sources(_yaml(tmp_path, texto))


RAIZ = Path(__file__).parents[2]


def _cnmc():
    return [s for s in load_sources(RAIZ / "config" / "data_sources.yml") if s.id.startswith("cnmc_")]


def test_las_fuentes_cnmc_se_leen_con_punto_y_coma_y_una_tabla_silver_snapshot_cada_una():
    fuentes = {s.id: s for s in _cnmc()}

    assert {s.options["delimiter"] for s in fuentes.values()} == {";"}
    assert {i: [(t.tabla, t.modo) for t in s.silver] for i, s in fuentes.items()} == {
        "cnmc_indicadores": [("cnmc_trimestral", "snapshot")],
        "cnmc_precio_trimestral": [("cnmc_precio_trimestral", "snapshot")],
        "cnmc_precio_mensual": [("cnmc_precio_mensual", "snapshot")],
    }
    assert all(s.url.startswith("https://catalogodatos.cnmc.es/") and s.url.endswith(".csv") for s in fuentes.values())
    assert all({"min_bytes", "min_filas", "columnas"} <= set(s.checks) for s in fuentes.values())


def test_cada_tabla_silver_declarada_tiene_su_sql_sus_gates_y_es_opcional():
    gates = yaml.safe_load((RAIZ / "config" / "quality_gates.yml").read_text(encoding="utf-8"))

    for fuente in load_sources(RAIZ / "config" / "data_sources.yml"):
        for tabla in fuente.silver:
            assert (RAIZ / "src" / "main" / "resources" / "silver" / f"{tabla.tabla}.sql").is_file(), tabla.tabla
            assert f"silver_{tabla.tabla}" in gates["tablas"], tabla.tabla
            assert f"silver_{tabla.tabla}" in gates["opcionales"], tabla.tabla


def test_las_columnas_que_exige_la_descarga_son_las_que_lee_el_sql_de_silver():
    for fuente in _cnmc():
        sql = (RAIZ / "src" / "main" / "resources" / "silver" / f"{fuente.silver[0].tabla}.sql").read_text(encoding="utf-8")
        for columna in fuente.checks["columnas"]:
            assert f"`{columna}`" in sql, f"{fuente.id}: el SQL de {fuente.silver[0].tabla} no usa la columna «{columna}»"

def test_load_sources_rejects_unknown_downloader(tmp_path):
    yml = tmp_path / "data_sources.yml"
    yml.write_text(
        """sources:
  - id: x
    name: X
    url: https://x.invalid/d
    format: json
    downloader: inventado
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="downloader no soportado"):
        load_sources(yml)


def test_load_sources_rejects_auth_without_env(tmp_path):
    yml = tmp_path / "data_sources.yml"
    yml.write_text(
        """sources:
  - id: x
    name: X
    url: https://x.invalid/d
    format: json
    downloader: nap
    auth: {header: ApiKey}
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="necesita 'env'"):
        load_sources(yml)


def test_load_sources_defaults_to_http_without_auth(tmp_path):
    yml = tmp_path / "data_sources.yml"
    yml.write_text(
        """sources:
  - id: x
    name: X
    url: https://x.invalid/d
    format: json
""",
        encoding="utf-8",
    )

    fuente = load_sources(yml)[0]

    assert fuente.downloader == "http"
    assert fuente.auth == {}


def test_cada_formato_solo_admite_sus_propias_opciones_de_lectura(tmp_path):
    # `delimiter` no significa nada en un xml, ni `rowTag` en un csv: es un error de configuración,
    # no una opción que se pueda ignorar en silencio.
    xml_con_delimiter = (
        "sources:\n  - id: f\n    name: x\n    url: https://example.invalid/a.xml\n"
        "    format: xml\n    options: {rowTag: documento, delimiter: ';'}\n"
    )
    with pytest.raises(ValueError, match="el formato 'xml' no admite delimiter"):
        load_sources(_yaml(tmp_path, xml_con_delimiter))

    csv_con_rowtag = (
        "sources:\n  - id: f\n    name: x\n    url: https://example.invalid/a.csv\n"
        "    format: csv\n    options: {rowTag: documento}\n"
    )
    with pytest.raises(ValueError, match="el formato 'csv' no admite rowTag"):
        load_sources(_yaml(tmp_path, csv_con_rowtag))


def test_checks_solo_se_admiten_en_fuentes_csv(tmp_path):
    texto = (
        "sources:\n  - id: f\n    name: x\n    url: https://example.invalid/a.json\n"
        "    format: json\n    checks: {min_bytes: 10}\n"
    )

    with pytest.raises(ValueError, match="'checks' solo se admite en fuentes csv"):
        load_sources(_yaml(tmp_path, texto))


def test_una_fuente_xml_necesita_declarar_rowtag(tmp_path):
    # Sin rowTag el lector de Spark no sabe qué elemento es una fila: la fuente no se podría leer.
    texto = "sources:\n  - id: f\n    name: x\n    url: https://example.invalid/a.xml\n    format: xml\n"

    with pytest.raises(ValueError, match="necesita options.rowTag"):
        load_sources(_yaml(tmp_path, texto))


def test_load_sources_acepta_xml_con_rowtag(tmp_path):
    texto = (
        "sources:\n  - id: f\n    name: x\n    url: https://example.invalid/a.xml\n"
        "    format: xml\n    options: {rowTag: documento}\n"
    )

    assert load_sources(_yaml(tmp_path, texto))[0].options == {"rowTag": "documento"}
