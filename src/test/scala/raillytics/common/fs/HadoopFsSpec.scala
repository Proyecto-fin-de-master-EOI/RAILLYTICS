package raillytics.common.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

// Rutas con espacios. No es una hipótesis: el 2026-10-02 una carga murió con
// "URISyntaxException: Illegal character in path at index 15" porque el proyecto vivía en
// "E:/Master Datos Laura/...". Se arregló en L1 ese mismo día y aquí quedó sin arreglar hasta el
// 2026-10-14, así que esto fija el comportamiento en los dos sitios.
class HadoopFsSpec extends AnyFlatSpec with Matchers {

  private val conf = new Configuration()

  "HadoopFs.forRoot" should "accept a local root with spaces, which is what `new URI(root)` rejected" in {
    val raiz = Files.createTempDirectory("raillytics con espacios")
    try {
      val fs = HadoopFs.forRoot(s"file:///${raiz.toString.replace('\\', '/')}", conf)

      fs.getScheme shouldBe "file"
    } finally Files.deleteIfExists(raiz)
  }

  it should "still resolve an s3a root to its own filesystem" in {
    HadoopFs.forRoot("s3a://raillytics-bronze", conf).getScheme shouldBe "s3a"
  }

  "the path of a file in a directory with spaces" should "keep its source and name" in {
    // Es lo que hace L1 con lo que le entrega `binaryFile`: la fuente es la carpeta padre.
    val uri = "file:/E:/Master Datos Laura/Proyecto/data/bronze/renfe_trip_updates/20261002_152002_x.json"

    new Path(uri).getParent.getName shouldBe "renfe_trip_updates"
    new Path(uri).getName shouldBe "20261002_152002_x.json"
  }
}
