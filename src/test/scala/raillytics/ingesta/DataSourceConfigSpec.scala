package raillytics.ingesta

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
}
