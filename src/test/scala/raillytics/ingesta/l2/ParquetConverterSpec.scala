package raillytics.ingesta.l2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.common.lake.BronzePaths
import raillytics.ingesta.config.DataSource
import raillytics.testutil.{TestPaths, TestSpark}

import java.io.FileOutputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.jdk.CollectionConverters._

class ParquetConverterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = TestSpark.session("ParquetConverterSpec")
    // startQuery sobre binaryFile (zip) exige esquema explícito o inferencia
    // habilitada (ver SparkSessionFactory.buildForFileStreaming); csv/json llevan
    // esquema explícito desde ParquetConverter.inferSchema.
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
  }

  override def afterAll(): Unit = spark.stop()

  private val today = java.time.LocalDate.now()

  private def settingsIn(tmpDir: Path): ParquetConverterSettings = ParquetConverterSettings(
    configPath = "unused",
    l1DoneRoot = tmpDir.resolve("l1_done").toString,
    processedRoot = tmpDir.resolve("processed").toString,
    rejectedRoot = tmpDir.resolve("rejected").toString,
    checkpointRoot = tmpDir.resolve("checkpoints").toString,
    bronzeRoot = TestPaths.fileUri(tmpDir.resolve("bronze")),
    cargasDir = TestPaths.fileUri(tmpDir.resolve("cargas")),
    calidadDir = TestPaths.fileUri(tmpDir.resolve("calidad")),
    pendingRetryMs = 30000L
  )

  private def csvBatch(source: DataSource, settings: ParquetConverterSettings) = {
    val input = s"${settings.l1DoneRoot}/${source.id}/*"
    spark.read.format("csv").option("header", "true")
      .schema(ParquetConverter.inferSchema(source, input))
      .option("columnNameOfCorruptRecord", ParquetConverter.CorruptRecordCol)
      .load(input)
      .withColumn(ParquetConverter.SourceFileCol, input_file_name())
  }

  private def trazas(dir: String, columnas: String*) =
    spark.read.parquet(dir).select(columnas.head, columnas.tail: _*).collect().map(_.toSeq)

  private def zipConMiembros(dest: Path, miembros: (String, String)*): Unit = {
    val out = new ZipOutputStream(new FileOutputStream(dest.toFile))
    try miembros.foreach { case (nombre, contenido) =>
      out.putNextEntry(new ZipEntry(nombre))
      out.write(contenido.getBytes(UTF_8))
      out.closeEntry()
    } finally out.close()
  }

  "ParquetConverter.processBatch" should "convert csv to parquet, move the file to processedRoot and leave a marker" in {
    val tmpDir = Files.createTempDirectory("parquet-converter-spec")
    val settings = settingsIn(tmpDir)
    val l1DoneDir = tmpDir.resolve("l1_done/crtm")
    Files.createDirectories(l1DoneDir)
    Files.writeString(l1DoneDir.resolve("sample.csv"), "estacion,viajeros\nAtocha,100\nSol,50\n")
    val source = DataSource("crtm", "CRTM test", "https://example.invalid", "csv")

    ParquetConverter.processBatch(source, csvBatch(source, settings), 0L, spark.sparkContext.hadoopConfiguration, settings)

    val parquetDir = tmpDir.resolve(s"bronze/l2/crtm/$today")
    Files.list(parquetDir).iterator().asScala.exists(_.toString.endsWith(".parquet")) shouldBe true
    val result = spark.read.parquet(BronzePaths.l2(settings.bronzeRoot, "crtm", today))
    result.count() shouldBe 2
    result.columns should contain theSameElementsAs Seq("estacion", "viajeros")   // sin _source_file ni _corrupt_record

    Files.exists(l1DoneDir.resolve("sample.csv")) shouldBe false
    Files.exists(tmpDir.resolve("processed/crtm/sample.csv")) shouldBe true
    Files.exists(tmpDir.resolve("checkpoints/l2/crtm/raillytics-batches/0")) shouldBe true

    // Trazabilidad: una fila por micro-batch y fuente, con las filas escritas en Parquet.
    trazas(settings.cargasDir, "proceso", "capa", "tabla", "filas", "destino", "estado") shouldBe Array(
      Seq("bronze_l2_parquet_converter", "bronze", "crtm", 2L, BronzePaths.l2(settings.bronzeRoot, "crtm", today), "ok"))
    // Quality gates del batch: cabecera, registros corruptos y filas convertidas, todos ok.
    trazas(settings.calidadDir, "tabla", "gate", "resultado").map(_.toList).toSet shouldBe Set(
      List("crtm", "cabecera_csv", "ok"), List("crtm", "registros_corruptos", "ok"), List("crtm", "filas_convertidas", "ok"))
  }

  it should "quarantine a csv whose header does not match the query schema and one with corrupt rows" in {
    val tmpDir = Files.createTempDirectory("parquet-converter-gates-spec")
    val settings = settingsIn(tmpDir)
    val l1DoneDir = tmpDir.resolve("l1_done/crtm")
    Files.createDirectories(l1DoneDir)
    Files.writeString(l1DoneDir.resolve("a_bueno.csv"), "estacion,viajeros\nAtocha,100\n")
    Files.writeString(l1DoneDir.resolve("b_otra_cabecera.csv"), "viajeros,estacion\n7,Sol\n")   // mismas columnas, otro orden
    Files.writeString(l1DoneDir.resolve("c_corrupto.csv"), "estacion,viajeros\nChamartin,20,extra,columnas\n")
    val source = DataSource("crtm", "CRTM test", "https://example.invalid", "csv")

    ParquetConverter.processBatch(source, csvBatch(source, settings), 0L, spark.sparkContext.hadoopConfiguration, settings)

    val result = spark.read.parquet(BronzePaths.l2(settings.bronzeRoot, "crtm", today))
    result.collect().map(_.toSeq) shouldBe Array(Seq("Atocha", "100"))   // solo el fichero bueno
    Files.exists(tmpDir.resolve("processed/crtm/a_bueno.csv")) shouldBe true
    Files.exists(tmpDir.resolve("rejected/crtm/b_otra_cabecera.csv")) shouldBe true
    Files.readString(tmpDir.resolve("rejected/crtm/b_otra_cabecera.csv.rechazo.txt")) should include("cabecera")
    Files.exists(tmpDir.resolve("rejected/crtm/c_corrupto.csv")) shouldBe true
    Files.readString(tmpDir.resolve("rejected/crtm/c_corrupto.csv.rechazo.txt")) should include("corrupto")
    Files.list(l1DoneDir).iterator().asScala.toList shouldBe empty

    val cargas = trazas(settings.cargasDir, "tabla", "filas", "estado", "error").map(_.toList).toSet
    cargas should contain(List("crtm", 1L, "ok", null))
    cargas.count(_(2) == "error") shouldBe 2
    val gates = trazas(settings.calidadDir, "gate", "resultado", "valor").map(_.toList).toSet
    gates should contain allOf (List("cabecera_csv", "fallo", 1.0), List("registros_corruptos", "fallo", 1.0), List("filas_convertidas", "ok", 1.0))
  }

  it should "not write parquet twice when the same batch is replayed after a restart" in {
    val tmpDir = Files.createTempDirectory("parquet-converter-replay-spec")
    val settings = settingsIn(tmpDir)
    val l1DoneDir = tmpDir.resolve("l1_done/crtm")
    Files.createDirectories(l1DoneDir)
    Files.writeString(l1DoneDir.resolve("a.csv"), "estacion,viajeros\nAtocha,100\n")
    Files.writeString(l1DoneDir.resolve("b.csv"), "estacion,viajeros\nSol,50\n")
    val source = DataSource("crtm", "CRTM test", "https://example.invalid", "csv")
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    ParquetConverter.processBatch(source, csvBatch(source, settings), 7L, hadoopConf, settings)
    // Simula un proceso que murió después de escribir el marcador y mover a.csv, pero antes de mover b.csv.
    Files.move(tmpDir.resolve("processed/crtm/b.csv"), l1DoneDir.resolve("b.csv"))

    ParquetConverter.processBatch(source, csvBatch(source, settings), 7L, hadoopConf, settings)

    spark.read.parquet(BronzePaths.l2(settings.bronzeRoot, "crtm", today)).count() shouldBe 2   // no 3
    Files.exists(tmpDir.resolve("processed/crtm/b.csv")) shouldBe true
    Files.list(l1DoneDir).iterator().asScala.toList shouldBe empty
    spark.read.parquet(settings.cargasDir).count() shouldBe 1   // la repetición no registra otra carga
  }

  it should "convert every csv member of a zip source into its own parquet prefix" in {
    val tmpDir = Files.createTempDirectory("parquet-converter-zip-spec")
    val settings = settingsIn(tmpDir)
    val l1DoneDir = tmpDir.resolve("l1_done/crtm")
    Files.createDirectories(l1DoneDir)
    zipConMiembros(l1DoneDir.resolve("gtfs.zip"),
      "stops.txt" -> "stop_id,stop_name\n1,Atocha\n2,Sol\n",
      "routes.txt" -> "route_id,route_short_name\nR1,C-2\n",
      "readme.md" -> "no soy csv\n")
    val source = DataSource("crtm", "CRTM test", "https://example.invalid", "zip")
    val batch = spark.read.format("binaryFile").load(s"${settings.l1DoneRoot}/crtm/*")

    ParquetConverter.processBatch(source, batch, 0L, spark.sparkContext.hadoopConfiguration, settings)

    val stops = spark.read.parquet(BronzePaths.l2Member(settings.bronzeRoot, "crtm", today, "stops"))
    stops.columns should contain theSameElementsAs Seq("stop_id", "stop_name")
    stops.count() shouldBe 2
    spark.read.parquet(BronzePaths.l2Member(settings.bronzeRoot, "crtm", today, "routes")).count() shouldBe 1
    Files.exists(tmpDir.resolve(s"bronze/l2/crtm/$today/readme")) shouldBe false
    Files.exists(tmpDir.resolve("processed/crtm/gtfs.zip")) shouldBe true

    trazas(settings.cargasDir, "tabla", "filas", "estado").map(_.toList).toSet shouldBe Set(
      List("crtm/stops", 2L, "ok"), List("crtm/routes", 1L, "ok"))
    trazas(settings.calidadDir, "gate", "resultado").map(_.toList).toSet shouldBe Set(
      List("zip_valido", "ok"), List("registros_corruptos", "ok"))
  }

  it should "quarantine a zip that is not a zip or has a corrupt member" in {
    val tmpDir = Files.createTempDirectory("parquet-converter-zip-bad-spec")
    val settings = settingsIn(tmpDir)
    val l1DoneDir = tmpDir.resolve("l1_done/crtm")
    Files.createDirectories(l1DoneDir)
    Files.writeString(l1DoneDir.resolve("a_falso.zip"), "<html>no soy un zip</html>")
    zipConMiembros(l1DoneDir.resolve("b_corrupto.zip"), "stops.txt" -> "stop_id,stop_name\n1,Atocha,de,mas\n")
    val source = DataSource("crtm", "CRTM test", "https://example.invalid", "zip")
    val batch = spark.read.format("binaryFile").load(s"${settings.l1DoneRoot}/crtm/*")

    ParquetConverter.processBatch(source, batch, 0L, spark.sparkContext.hadoopConfiguration, settings)

    Files.exists(tmpDir.resolve(s"bronze/l2/crtm")) shouldBe false   // nada escrito
    Files.exists(tmpDir.resolve("rejected/crtm/a_falso.zip")) shouldBe true
    Files.exists(tmpDir.resolve("rejected/crtm/b_corrupto.zip")) shouldBe true
    Files.readString(tmpDir.resolve("rejected/crtm/b_corrupto.zip.rechazo.txt")) should include("stops")
    trazas(settings.cargasDir, "estado").flatten.toSet shouldBe Set("error")
  }

  "ParquetConverter.startQuery" should "parse a multi-line JSON source (GTFS-RT-shaped) using multiLine=true" in {
    val tmpDir = Files.createTempDirectory("parquet-converter-json-spec")
    val settings = settingsIn(tmpDir)
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
    val source = DataSource("renfe_trip_updates", "Renfe test", "https://example.invalid", "json")

    val query = ParquetConverter.startQuery(source, settings, spark.sparkContext.hadoopConfiguration)
    query.processAllAvailable()
    query.stop()

    val result = spark.read.parquet(BronzePaths.l2(settings.bronzeRoot, source.id, today))
    // Un único fichero con un único objeto JSON (no JSON-Lines) -> una fila,
    // con "entity"/"header" anidados tal cual venían. Sin multiLine=true esto
    // habría producido varias filas con la columna _corrupt_record.
    result.count() shouldBe 1
    result.columns should contain allOf ("entity", "header")
    result.columns should not contain ParquetConverter.CorruptRecordCol

    Files.exists(l1DoneDir.resolve("sample.json")) shouldBe false
    Files.exists(tmpDir.resolve("processed/renfe_trip_updates/sample.json")) shouldBe true
  }

  it should "quarantine a json file that is not valid json instead of writing garbage" in {
    val tmpDir = Files.createTempDirectory("parquet-converter-json-bad-spec")
    val settings = settingsIn(tmpDir)
    val l1DoneDir = tmpDir.resolve("l1_done/renfe_trip_updates")
    Files.createDirectories(l1DoneDir)
    Files.writeString(l1DoneDir.resolve("a_bueno.json"), """{ "entity": [ { "id": "a" } ] }""")
    Files.writeString(l1DoneDir.resolve("b_html.json"), "<html><body>proxy error</body></html>")
    val source = DataSource("renfe_trip_updates", "Renfe test", "https://example.invalid", "json")

    val query = ParquetConverter.startQuery(source, settings, spark.sparkContext.hadoopConfiguration)
    query.processAllAvailable()
    query.stop()

    spark.read.parquet(BronzePaths.l2(settings.bronzeRoot, source.id, today)).count() shouldBe 1
    Files.exists(tmpDir.resolve("processed/renfe_trip_updates/a_bueno.json")) shouldBe true
    Files.exists(tmpDir.resolve("rejected/renfe_trip_updates/b_html.json")) shouldBe true
  }

  "ParquetConverter.startAll" should "keep a source without L1 files pending and start it once files arrive" in {
    val tmpDir = Files.createTempDirectory("parquet-converter-pending-spec")
    val settings = settingsIn(tmpDir)
    val source = DataSource("renfe_vehicle_positions", "Renfe test", "https://example.invalid", "json")
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // L2 arrancado antes que L1: ni siquiera existe el directorio de la fuente.
    ParquetConverter.startAll(Seq(source), settings, hadoopConf) shouldBe Seq(source)
    spark.streams.active shouldBe empty

    // Existe pero está vacío (estado normal en reposo): sigue pendiente.
    val l1DoneDir = tmpDir.resolve("l1_done/renfe_vehicle_positions")
    Files.createDirectories(l1DoneDir)
    ParquetConverter.startAll(Seq(source), settings, hadoopConf) shouldBe Seq(source)
    spark.streams.active shouldBe empty

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
