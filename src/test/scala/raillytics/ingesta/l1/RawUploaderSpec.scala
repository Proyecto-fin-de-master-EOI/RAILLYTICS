package raillytics.ingesta.l1

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.testutil.{TestPaths, TestSpark}

import java.nio.file.{Files, Path}

class RawUploaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = TestSpark.session("RawUploaderSpec")
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
  }

  override def afterAll(): Unit = spark.stop()

  private def settingsIn(tmpDir: Path): RawUploaderSettings = RawUploaderSettings(
    stagingRoot = tmpDir.resolve("staging").toString,
    l1DoneRoot = tmpDir.resolve("l1_done").toString,
    checkpointRoot = tmpDir.resolve("checkpoints").toString,
    bronzeRoot = TestPaths.fileUri(tmpDir.resolve("bronze")),
    cargasDir = TestPaths.fileUri(tmpDir.resolve("cargas")),
    calidadDir = TestPaths.fileUri(tmpDir.resolve("calidad"))
  )

  "RawUploader.processBatch" should "copy the file as-is to bronzeRoot, move it to l1DoneRoot and check its size" in {
    val tmpDir: Path = Files.createTempDirectory("raw-uploader-spec")
    val settings = settingsIn(tmpDir)
    val stagingDir = tmpDir.resolve("staging/crtm")
    Files.createDirectories(stagingDir)
    Files.writeString(stagingDir.resolve("sample.csv"), "estacion,viajeros\nAtocha,100\n")

    val batch = spark.read.format("binaryFile").load(s"${settings.stagingRoot}/*/*")
    RawUploader.processBatch(batch, spark.sparkContext.hadoopConfiguration, settings)

    val today = java.time.LocalDate.now()
    val expectedRawFile = tmpDir.resolve(s"bronze/l1-raw/crtm/$today/sample.csv")
    Files.exists(expectedRawFile) shouldBe true
    Files.readString(expectedRawFile) shouldBe "estacion,viajeros\nAtocha,100\n"

    Files.exists(stagingDir.resolve("sample.csv")) shouldBe false
    Files.exists(tmpDir.resolve("l1_done/crtm/sample.csv")) shouldBe true

    // Trazabilidad: una fila por fichero subido, con su tamaño y su destino en Bronze.
    val traza = spark.read.parquet(settings.cargasDir).select("proceso", "capa", "tabla", "bytes", "destino", "estado").collect()
    traza should have length 1
    traza.head.toSeq shouldBe Seq("bronze_l1_raw_uploader", "bronze", "crtm", Files.size(expectedRawFile),
      s"${settings.bronzeRoot}/l1-raw/crtm/$today/sample.csv", "ok")
    // Quality gate: los bytes subidos coinciden con los del origen.
    val gate = spark.read.parquet(settings.calidadDir).select("proceso", "tabla", "gate", "severidad", "resultado", "valor", "umbral").collect()
    gate.map(_.toSeq) shouldBe Array(Seq("bronze_l1_raw_uploader", "crtm", "bytes_subidos", "bloqueante", "ok",
      Files.size(expectedRawFile).toDouble, s"= ${Files.size(expectedRawFile)}"))
  }

  it should "finish a replayed batch instead of dying: skip moved files, re-copy the rest" in {
    val tmpDir: Path = Files.createTempDirectory("raw-uploader-replay-spec")
    val settings = settingsIn(tmpDir)
    val stagingDir = tmpDir.resolve("staging/crtm")
    Files.createDirectories(stagingDir)
    Files.writeString(stagingDir.resolve("a.csv"), "a\n1\n")
    Files.writeString(stagingDir.resolve("b.csv"), "b\n2\n")
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // El batch de Spark tiene los dos ficheros; se simula una ejecución anterior que
    // subió y movió a.csv y subió b.csv (sin llegar a moverlo) antes de morir.
    val batch = spark.read.format("binaryFile").load(s"${settings.stagingRoot}/*/*")
    batch.count()
    val today = java.time.LocalDate.now()
    val rawDir = tmpDir.resolve(s"bronze/l1-raw/crtm/$today")
    Files.createDirectories(rawDir)
    Files.copy(stagingDir.resolve("a.csv"), rawDir.resolve("a.csv"))
    Files.copy(stagingDir.resolve("b.csv"), rawDir.resolve("b.csv"))
    Files.createDirectories(tmpDir.resolve("l1_done/crtm"))
    Files.move(stagingDir.resolve("a.csv"), tmpDir.resolve("l1_done/crtm/a.csv"))

    RawUploader.processBatch(batch, hadoopConf, settings)

    Files.exists(tmpDir.resolve("l1_done/crtm/a.csv")) shouldBe true
    Files.exists(tmpDir.resolve("l1_done/crtm/b.csv")) shouldBe true
    Files.exists(stagingDir.resolve("b.csv")) shouldBe false
    Files.readString(rawDir.resolve("b.csv")) shouldBe "b\n2\n"
    // Solo b.csv se ha vuelto a subir: una única fila de trazabilidad.
    spark.read.parquet(settings.cargasDir).select("tabla", "origen").collect().map(_.getString(1)) should have length 1
  }
}
