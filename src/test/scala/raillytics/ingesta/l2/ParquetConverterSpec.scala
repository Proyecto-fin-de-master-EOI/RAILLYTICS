package raillytics.ingesta.l2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.common.lake.BronzePaths
import raillytics.ingesta.config.DataSource
import raillytics.testutil.{TestPaths, TestSpark}

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class ParquetConverterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = TestSpark.session("ParquetConverterSpec")
    // startQuery usa readStream, que exige esquema explícito o inferencia
    // habilitada (ver SparkSessionFactory.buildForFileStreaming).
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
  }

  override def afterAll(): Unit = spark.stop()

  "ParquetConverter.processBatch" should "convert csv to parquet and move the file to processedRoot" in {
    val tmpDir: Path = Files.createTempDirectory("parquet-converter-spec")
    val l1DoneDir = tmpDir.resolve("l1_done/crtm")
    Files.createDirectories(l1DoneDir)
    Files.writeString(l1DoneDir.resolve("sample.csv"), "estacion,viajeros\nAtocha,100\nSol,50\n")

    val bronzeRoot = TestPaths.fileUri(tmpDir.resolve("bronze"))
    val processedRoot = tmpDir.resolve("processed").toString
    val cargasDir = TestPaths.fileUri(tmpDir.resolve("cargas"))
    val source = DataSource("crtm", "CRTM test", "https://example.invalid", "csv")

    val batch = spark.read.format("csv").option("header", "true")
      .load(s"${tmpDir.resolve("l1_done/crtm")}/*")
      .withColumn(ParquetConverter.SourceFileCol, input_file_name())

    ParquetConverter.processBatch(source, batch, spark.sparkContext.hadoopConfiguration, bronzeRoot, processedRoot, cargasDir)

    val today = java.time.LocalDate.now()
    val parquetDir = tmpDir.resolve(s"bronze/l2/crtm/$today")
    Files.exists(parquetDir) shouldBe true
    Files.list(parquetDir).iterator().asScala.exists(_.toString.endsWith(".parquet")) shouldBe true

    val result = spark.read.parquet(BronzePaths.l2(bronzeRoot, "crtm", today))
    result.count() shouldBe 2
    result.columns should contain theSameElementsAs Seq("estacion", "viajeros")

    Files.exists(l1DoneDir.resolve("sample.csv")) shouldBe false
    Files.exists(tmpDir.resolve("processed/crtm/sample.csv")) shouldBe true

    // Trazabilidad: una fila por micro-batch y fuente, con las filas escritas en Parquet.
    val traza = spark.read.parquet(cargasDir).select("proceso", "capa", "tabla", "filas", "destino", "estado").collect()
    traza should have length 1
    traza.head.toSeq shouldBe Seq("bronze_l2_parquet_converter", "bronze", "crtm", 2L, BronzePaths.l2(bronzeRoot, "crtm", today), "ok")
  }

  "ParquetConverter.startQuery" should "parse a multi-line JSON source (GTFS-RT-shaped) using multiLine=true" in {
    val tmpDir: Path = Files.createTempDirectory("parquet-converter-json-spec")
    val l1DoneDir = tmpDir.resolve("l1_done/renfe_trip_updates")
    Files.createDirectories(l1DoneDir)
    Files.writeString(
      l1DoneDir.resolve("sample.json"),
      """{
        |  "header": { "gtfsRealtimeVersion": "2.0", "timestamp": "123" },
        |  "entity": [
        |    { "id": "a", "tripUpdate": { "trip": { "tripId": "t1" } } },
        |    { "id": "b", "tripUpdate": { "trip": { "tripId": "t2" } } }
        |  ]
        |}
        |""".stripMargin
    )

    val bronzeRoot = TestPaths.fileUri(tmpDir.resolve("bronze"))
    val settings = ParquetConverterSettings(
      configPath = "unused",
      l1DoneRoot = tmpDir.resolve("l1_done").toString,
      processedRoot = tmpDir.resolve("processed").toString,
      checkpointRoot = tmpDir.resolve("checkpoints").toString,
      bronzeRoot = bronzeRoot,
      cargasDir = TestPaths.fileUri(tmpDir.resolve("cargas")),
      pendingRetryMs = 30000L
    )
    val source = DataSource("renfe_trip_updates", "Renfe test", "https://example.invalid", "json")

    val query = ParquetConverter.startQuery(source, settings, spark.sparkContext.hadoopConfiguration)
    query.processAllAvailable()
    query.stop()

    val today = java.time.LocalDate.now()
    val result = spark.read.parquet(BronzePaths.l2(bronzeRoot, source.id, today))
    // Un único fichero con un único objeto JSON (no JSON-Lines) -> una fila,
    // con "entity"/"header" anidados tal cual venían. Sin multiLine=true esto
    // habría producido varias filas con la columna _corrupt_record.
    result.count() shouldBe 1
    result.columns should contain allOf ("entity", "header")

    Files.exists(l1DoneDir.resolve("sample.json")) shouldBe false
    Files.exists(tmpDir.resolve("processed/renfe_trip_updates/sample.json")) shouldBe true
  }

  "ParquetConverter.startAll" should "keep a source without L1 files pending and start it once files arrive" in {
    val tmpDir: Path = Files.createTempDirectory("parquet-converter-pending-spec")
    val settings = ParquetConverterSettings(
      configPath = "unused",
      l1DoneRoot = tmpDir.resolve("l1_done").toString,
      processedRoot = tmpDir.resolve("processed").toString,
      checkpointRoot = tmpDir.resolve("checkpoints").toString,
      bronzeRoot = TestPaths.fileUri(tmpDir.resolve("bronze")),
      cargasDir = TestPaths.fileUri(tmpDir.resolve("cargas")),
      pendingRetryMs = 30000L
    )
    val source = DataSource("renfe_vehicle_positions", "Renfe test", "https://example.invalid", "json")
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // L2 arrancado antes que L1: ni siquiera existe el directorio de la fuente.
    ParquetConverter.startAll(Seq(source), settings, hadoopConf) shouldBe Seq(source)
    spark.streams.active shouldBe empty

    val l1DoneDir = tmpDir.resolve("l1_done/renfe_vehicle_positions")
    Files.createDirectories(l1DoneDir)
    Files.writeString(l1DoneDir.resolve("sample.json"), """{ "entity": [ { "id": "a" } ] }""")

    ParquetConverter.startAll(Seq(source), settings, hadoopConf) shouldBe empty
    spark.streams.active should have length 1
    spark.streams.active.foreach { query =>
      query.processAllAvailable()
      query.stop()
    }
    Files.exists(tmpDir.resolve("processed/renfe_vehicle_positions/sample.json")) shouldBe true
  }
}
