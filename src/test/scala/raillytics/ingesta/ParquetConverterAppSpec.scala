package raillytics.ingesta

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class ParquetConverterAppSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("ParquetConverterAppSpec").master("local[1]").getOrCreate()
    // startQuery usa readStream, que exige esquema explícito o inferencia
    // habilitada (ver la misma config en ParquetConverterApp.main()).
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
  }

  override def afterAll(): Unit = spark.stop()

  "ParquetConverterApp.processBatch" should "convert csv to parquet and move the file to processedRoot" in {
    val tmpDir: Path = Files.createTempDirectory("parquet-converter-spec")
    val l1DoneDir = tmpDir.resolve("l1_done/crtm")
    Files.createDirectories(l1DoneDir)
    Files.writeString(l1DoneDir.resolve("sample.csv"), "estacion,viajeros\nAtocha,100\nSol,50\n")

    val bronzeRoot = s"file://${tmpDir.resolve("bronze")}"
    val processedRoot = tmpDir.resolve("processed").toString
    val source = DataSource("crtm", "CRTM test", "https://example.invalid", "csv")

    val batch = spark.read.format("csv").option("header", "true")
      .load(s"${tmpDir.resolve("l1_done/crtm")}/*")
      .withColumn(ParquetConverterApp.SourceFileCol, input_file_name())

    ParquetConverterApp.processBatch(source, batch, spark.sparkContext.hadoopConfiguration, bronzeRoot, processedRoot)

    val today = java.time.LocalDate.now()
    val parquetDir = tmpDir.resolve(s"bronze/l2/crtm/$today")
    Files.exists(parquetDir) shouldBe true
    Files.list(parquetDir).iterator().asScala.exists(_.toString.endsWith(".parquet")) shouldBe true

    val result = spark.read.parquet(BronzePaths.l2(bronzeRoot, "crtm", today))
    result.count() shouldBe 2
    result.columns should contain theSameElementsAs Seq("estacion", "viajeros")

    Files.exists(l1DoneDir.resolve("sample.csv")) shouldBe false
    Files.exists(tmpDir.resolve("processed/crtm/sample.csv")) shouldBe true
  }

  "ParquetConverterApp.startQuery" should "parse a multi-line JSON source (GTFS-RT-shaped) using multiLine=true" in {
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

    val bronzeRoot = s"file://${tmpDir.resolve("bronze")}"
    val processedRoot = tmpDir.resolve("processed").toString
    val checkpointRoot = tmpDir.resolve("checkpoints").toString
    val l1DoneRoot = tmpDir.resolve("l1_done").toString
    val source = DataSource("renfe_trip_updates", "Renfe test", "https://example.invalid", "json")

    val query = ParquetConverterApp.startQuery(
      source, bronzeRoot, l1DoneRoot, processedRoot, checkpointRoot, spark.sparkContext.hadoopConfiguration
    )
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
}
