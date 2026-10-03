package raillytics.ingesta.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

class DataSourceConfigSpec extends AnyFlatSpec with Matchers {

  private def streamOf(yaml: String) =
    new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8))

  "DataSourceConfig.loadFromStream" should "parse a valid sources YAML" in {
    val yaml =
      """sources:
        |  - id: crtm
        |    name: "CRTM - Consorcio Regional de Transportes de Madrid"
        |    url: "https://crtm.maps.arcgis.com/sharing/rest/content/items/1a25440bf66f499bae2657ec7fb40144/data"
        |    format: csv
        |""".stripMargin

    DataSourceConfig.loadFromStream(streamOf(yaml)) shouldBe Seq(
      DataSource(
        id = "crtm",
        name = "CRTM - Consorcio Regional de Transportes de Madrid",
        url = "https://crtm.maps.arcgis.com/sharing/rest/content/items/1a25440bf66f499bae2657ec7fb40144/data",
        format = "csv"
      )
    )
  }

  it should "reject an unsupported format" in {
    val yaml =
      """sources:
        |  - id: bad
        |    name: Bad source
        |    url: "https://example.invalid/data"
        |    format: xlsx
        |""".stripMargin

    an[IllegalArgumentException] should be thrownBy DataSourceConfig.loadFromStream(streamOf(yaml))
  }

  it should "reject an entry missing a required field instead of silently producing a null" in {
    val yaml =
      """sources:
        |  - ids: bad
        |    name: Bad source
        |    url: "https://example.invalid/data"
        |    format: csv
        |""".stripMargin

    val exception = the[IllegalArgumentException] thrownBy DataSourceConfig.loadFromStream(streamOf(yaml))
    exception.getMessage should include("'id'")
  }

  private val cnmc =
    """sources:
      |  - id: cnmc
      |    name: "CNMC prueba"
      |    url: "https://example.invalid/ds.csv"
      |    format: csv
      |    options: {delimiter: ";"}
      |    checks:
      |      min_bytes: 1000
      |      min_filas: 5
      |      columnas: ["Trimestre", "Viajeros (Núm)"]
      |    silver:
      |      - {tabla: cnmc_trimestral, modo: snapshot}
      |""".stripMargin

  it should "parse options, checks and silver tables" in {
    val Seq(fuente) = DataSourceConfig.loadFromStream(streamOf(cnmc))

    fuente.options shouldBe Map("delimiter" -> ";")
    fuente.checks.keySet shouldBe Set("min_bytes", "min_filas", "columnas")
    fuente.silver shouldBe Seq(SilverTabla("cnmc_trimestral", SilverTabla.Snapshot))
    fuente.silver.head.esSnapshot shouldBe true
  }

  it should "give empty defaults to a source without extras" in {
    val yaml =
      """sources:
        |  - id: crtm
        |    name: CRTM
        |    url: "https://example.invalid/a"
        |    format: zip
        |""".stripMargin

    val Seq(fuente) = DataSourceConfig.loadFromStream(streamOf(yaml))

    (fuente.options, fuente.checks, fuente.silver) shouldBe ((Map.empty, Map.empty, Seq.empty))
  }

  private def conCambio(cambio: String): String =
    "sources:\n  - id: cnmc\n    name: x\n    url: \"https://example.invalid/a.csv\"\n    format: csv\n" + cambio

  it should "reject unknown keys, bad values and options on non-csv sources, saying what is wrong" in {
    val casos = Seq(
      "    optons: {delimiter: ';'}\n"            -> "Clave desconocida en la fuente 'cnmc': optons",
      "    options: {separator: ';'}\n"          -> "options",
      "    checks: {min_byte: 10}\n"             -> "checks",
      "    options: {delimiter: ';;'}\n"         -> "un único carácter",
      "    silver: [{tabla: Mal-Nombre, modo: snapshot}]\n" -> "nombre de tabla Silver inválido",
      "    silver: [{tabla: ok, modo: tiempo_real}]\n"      -> "modo Silver no soportado",
      "    silver: [{tabla: ok}]\n"              -> "'modo'"
    )
    casos.foreach { case (cambio, fragmento) =>
      val e = the[IllegalArgumentException] thrownBy DataSourceConfig.loadFromStream(streamOf(conCambio(cambio)))
      withClue(cambio) { e.getMessage should include(fragmento) }
    }
    val json = "sources:\n  - id: f\n    name: x\n    url: \"https://example.invalid/a.json\"\n    format: json\n    options: {delimiter: ';'}\n"
    (the[IllegalArgumentException] thrownBy DataSourceConfig.loadFromStream(streamOf(json))).getMessage should include("solo se admiten en fuentes csv")
  }

  it should "reject a silver table declared by more than one source" in {
    val una = "  - id: a\n    name: x\n    url: \"https://example.invalid/a.csv\"\n    format: csv\n    silver: [{tabla: t, modo: snapshot}]\n"
    val e = the[IllegalArgumentException] thrownBy DataSourceConfig.loadFromStream(streamOf("sources:\n" + una + una.replace("id: a", "id: b")))
    e.getMessage should include("más de una fuente: t")
  }

  it should "parse the project registry, including the three CNMC sources with their options and silver tables" in {
    val fuentes = DataSourceConfig.load("config/data_sources.yml").map(f => f.id -> f).toMap

    fuentes.keySet should contain allOf ("crtm", "renfe_trip_updates", "renfe_vehicle_positions", "cnmc_indicadores", "cnmc_precio_trimestral", "cnmc_precio_mensual")
    fuentes("cnmc_indicadores").options shouldBe Map("delimiter" -> ";")
    fuentes("cnmc_indicadores").silver shouldBe Seq(SilverTabla("cnmc_trimestral", SilverTabla.Snapshot))
    fuentes("cnmc_precio_mensual").silver.map(_.tabla) shouldBe Seq("cnmc_precio_mensual")
    fuentes("crtm").silver shouldBe empty
  }
}
