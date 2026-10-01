package raillytics.silver

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.common.lake.LakeSettings
import raillytics.ingesta.config.{DataSource, SilverTabla}
import raillytics.testutil.{TestPaths, TestSpark}

import java.nio.file.{Files, Path => NioPath}
import java.time.LocalDate
import scala.jdk.CollectionConverters._

class SilverBuilderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = TestSpark.session("SilverBuilderSpec")
    // Como SparkSessionFactory.buildForFileStreaming: readStream sobre ficheros exige esquema o inferencia habilitada.
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
  }

  override def afterAll(): Unit = spark.stop()

  private val hoy = LocalDate.now()
  private def hadoopConf = spark.sparkContext.hadoopConfiguration

  // Gates mínimos: con filas_minimas = 2 una foto de una sola fila es «mala» y se manda a cuarentena.
  private val GatesCnmc =
    """tablas:
      |  silver_cnmc_trimestral:
      |    - {nombre: filas_minimas, tipo: filas_min, minimo: 2, severidad: bloqueante}
      |    - {nombre: grano_unico, tipo: unico, columnas: [anio, trimestre, corredor, empresa], severidad: bloqueante}
      |    - {nombre: empresas_conocidas, tipo: sql, severidad: aviso, sql: "SELECT count(*) FROM silver_cnmc_trimestral WHERE operador_id = 'OTRO'"}
      |  silver_tabla_incremental_prueba:
      |    - {nombre: filas_minimas, tipo: filas_min, minimo: 1, severidad: bloqueante}
      |  silver_tabla_sql_roto_prueba:
      |    - {nombre: filas_minimas, tipo: filas_min, minimo: 1, severidad: bloqueante}
      |""".stripMargin

  private def settingsIn(tmpDir: NioPath): SilverSettings = {
    val gates = tmpDir.resolve("quality_gates.yml")
    Files.writeString(gates, GatesCnmc)
    SilverSettings(
      configPath = "unused",
      calidadConfig = gates.toString,
      checkpointRoot = tmpDir.resolve("checkpoints").toString,
      lake = LakeSettings(
        bronzeRoot = TestPaths.fileUri(tmpDir.resolve("bronze")),
        silverRoot = TestPaths.fileUri(tmpDir.resolve("silver")),
        goldRoot = TestPaths.fileUri(tmpDir.resolve("gold")),
        trazabilidadRoot = TestPaths.fileUri(tmpDir.resolve("gold/_trazabilidad"))),
      triggerIntervalMs = 100L,
      pendingRetryMs = 100L)
  }

  private val fuente = DataSource("cnmc_indicadores", "CNMC test", "https://example.invalid", "csv",
    options = Map("delimiter" -> ";"), silver = Seq(SilverTabla("cnmc_trimestral", SilverTabla.Snapshot)))
  private val tabla = fuente.silver.head

  private val ColumnasCnmc = Seq("Trimestre", "Tipo de producto", "Corredor", "Empresa", "Tipo de servicio",
    "Ingresos por venta de billetes (€)", "Plazas Ofertadas (Núm)", "Plazas.km Ofertadas (Plazas.km)", "Tren.km (Tren.km)",
    "Viajeros (Núm)", "Viajeros.km (Viajeros.km)")

  private def fila(trimestre: String, empresa: String, viajeros: String): Seq[String] =
    Seq(trimestre, "LD AV", "Madrid-Barcelona", empresa, "Comercial", "", "1000", "100000", "500", viajeros, "200000")

  // Lo que dejaría L2: Parquet con todas las columnas string, en l2/<fuente>/<fecha>/.
  private def escribirL2(settings: SilverSettings, fuenteId: String, fecha: String, columnas: Seq[String], filas: Seq[Seq[String]]): Unit = {
    val schema = StructType(columnas.map(StructField(_, StringType)))
    spark.createDataFrame(filas.map(Row.fromSeq).asJava, schema)
      .coalesce(1).write.mode("append").parquet(s"${settings.bronzeRoot}/l2/$fuenteId/$fecha/")
  }

  private def escribirCnmc(settings: SilverSettings, fecha: String, filas: Seq[Seq[String]]): Unit =
    escribirL2(settings, "cnmc_indicadores", fecha, ColumnasCnmc, filas)

  private def silver(settings: SilverSettings, nombre: String) = spark.read.parquet(settings.lake.silverTable(nombre))

  private def trazas(dir: String, columnas: String*) =
    spark.read.parquet(dir).select(columnas.head, columnas.tail: _*).collect().map(_.toSeq)

  private def batchDe(settings: SilverSettings, fuenteId: String) =
    spark.read.parquet(s"${settings.bronzeRoot}/l2/$fuenteId/*").withColumn(SilverBuilder.FuenteCol, input_file_name())

  "SilverBuilder.startQuery" should "rebuild a snapshot table from the latest L2 partition only, with traceability and gates" in {
    val tmp = Files.createTempDirectory("silver-builder-snapshot-spec")
    val settings = settingsIn(tmp)
    escribirCnmc(settings, "2026-09-30", Seq(fila("2026T1", "Renfe Viajeros", "100"), fila("2026T1", "Iryo", "50")))
    escribirCnmc(settings, "2026-10-01", Seq(fila("2026T1", "Renfe Viajeros", "110"), fila("2026T1", "Iryo", "55"), fila("2026T2", "Renfe Viajeros", "120")))

    val query = SilverBuilder.startQuery(fuente, tabla, settings, hadoopConf)
    query.processAllAvailable()
    query.stop()

    silver(settings, "cnmc_trimestral").select("anio", "trimestre", "operador_id", "viajeros").collect().map(_.toSeq).toSet shouldBe
      Set(Seq(2026, 1, "RENFE", 110L), Seq(2026, 1, "IRYO", 55L), Seq(2026, 2, "RENFE", 120L))   // nada de la partición vieja (100 y 50)
    trazas(settings.cargasDir, "proceso", "capa", "tabla", "filas", "estado") shouldBe
      Array(Seq("silver_builder", "silver", "cnmc_trimestral", 3L, "ok"))
    trazas(settings.calidadDir, "tabla", "gate", "resultado").map(_.toList).toSet shouldBe Set(
      List("silver_cnmc_trimestral", "filas_minimas", "ok"), List("silver_cnmc_trimestral", "grano_unico", "ok"),
      List("silver_cnmc_trimestral", "empresas_conocidas", "ok"))
    Files.exists(tmp.resolve("checkpoints/silver/cnmc_trimestral/raillytics-batches/0")) shouldBe true
  }

  it should "quarantine a snapshot that fails a blocking gate, leave Silver as it was and keep the stream alive" in {
    val tmp = Files.createTempDirectory("silver-builder-cuarentena-spec")
    val settings = settingsIn(tmp)
    escribirCnmc(settings, "2026-09-30", Seq(fila("2026T1", "Renfe Viajeros", "100"), fila("2026T1", "Iryo", "50")))
    val query = SilverBuilder.startQuery(fuente, tabla, settings, hadoopConf)
    query.processAllAvailable()
    silver(settings, "cnmc_trimestral").count() shouldBe 2

    // La foto más NUEVA trae una sola fila: no pasa filas_minimas (≥ 2).
    escribirCnmc(settings, "2026-10-01", Seq(fila("2026T2", "Renfe Viajeros", "999")))
    query.processAllAvailable()

    query.isActive shouldBe true
    query.exception shouldBe None
    query.stop()
    silver(settings, "cnmc_trimestral").select("viajeros").collect().map(_.getLong(0)).toSet shouldBe Set(100L, 50L)   // intacto
    val cuarentena = tmp.resolve(s"silver/_cuarentena/cnmc_trimestral/$hoy-1")
    Files.exists(cuarentena) shouldBe true
    spark.read.parquet(cuarentena.toString).select("viajeros").collect().map(_.getLong(0)) shouldBe Array(999L)
    val cargas = trazas(settings.cargasDir, "tabla", "estado", "error").map(_.toList)
    cargas.count(_(1) == "error") shouldBe 1
    cargas.find(_(1) == "error").get(2).asInstanceOf[String] should (include("cuarentena") and include("filas_minimas"))
    trazas(settings.calidadDir, "gate", "resultado").map(_.toList).toSet should contain(List("filas_minimas", "fallo"))
    Files.exists(tmp.resolve("checkpoints/silver/cnmc_trimestral/raillytics-batches/1")) shouldBe true   // el batch queda resuelto
  }

  "SilverBuilder.procesarBatch" should "not write twice when the same batch is replayed after a restart" in {
    val tmp = Files.createTempDirectory("silver-builder-replay-spec")
    val settings = settingsIn(tmp)
    escribirCnmc(settings, "2026-10-01", Seq(fila("2026T1", "Renfe Viajeros", "100"), fila("2026T1", "Iryo", "50")))
    val batch = batchDe(settings, "cnmc_indicadores")

    SilverBuilder.procesarBatch(fuente, tabla, batch, 0L, hadoopConf, settings)
    silver(settings, "cnmc_trimestral").count() shouldBe 2
    val destino = tmp.resolve("silver/cnmc_trimestral")
    org.apache.hadoop.fs.FileUtil.fullyDelete(destino.toFile)
    SilverBuilder.procesarBatch(fuente, tabla, batch, 0L, hadoopConf, settings)

    Files.exists(destino) shouldBe false   // el marcador evita reescribir
    trazas(settings.cargasDir, "tabla").length shouldBe 1
  }

  it should "append the batch in incremental mode and expose the ingestion date of each file" in {
    val tmp = Files.createTempDirectory("silver-builder-incremental-spec")
    val settings = settingsIn(tmp)
    val incremental = DataSource("feed", "Feed test", "https://example.invalid", "csv", silver = Seq(SilverTabla("tabla_incremental_prueba", SilverTabla.Incremental)))
    val columnas = Seq("Trimestre", "Viajeros (Núm)")
    escribirL2(settings, "feed", "2026-09-30", columnas, Seq(Seq("2026T1", "10"), Seq("2026T2", "20")))
    SilverBuilder.procesarBatch(incremental, incremental.silver.head, batchDe(settings, "feed"), 0L, hadoopConf, settings)
    escribirL2(settings, "feed", "2026-10-01", columnas, Seq(Seq("2026T3", "30")))
    val nuevo = batchDe(settings, "feed").filter(org.apache.spark.sql.functions.col(SilverBuilder.FuenteCol).contains("/2026-10-01/"))

    SilverBuilder.procesarBatch(incremental, incremental.silver.head, nuevo, 1L, hadoopConf, settings)

    silver(settings, "tabla_incremental_prueba").collect().map(r => (r.getString(0), r.getString(1), r.getDate(2).toString)).toSet shouldBe
      Set(("2026T1", "10", "2026-09-30"), ("2026T2", "20", "2026-09-30"), ("2026T3", "30", "2026-10-01"))
  }

  it should "fail loudly, without a marker, when the SQL itself is broken (a code error, not a data error)" in {
    val tmp = Files.createTempDirectory("silver-builder-sql-roto-spec")
    val settings = settingsIn(tmp)
    val roto = DataSource("feed", "Feed test", "https://example.invalid", "csv", silver = Seq(SilverTabla("tabla_sql_roto_prueba", SilverTabla.Snapshot)))
    escribirL2(settings, "feed", "2026-10-01", Seq("Trimestre"), Seq(Seq("2026T1")))

    an[AnalysisException] should be thrownBy
      SilverBuilder.procesarBatch(roto, roto.silver.head, batchDe(settings, "feed"), 0L, hadoopConf, settings)

    Files.exists(tmp.resolve("checkpoints/silver/tabla_sql_roto_prueba/raillytics-batches/0")) shouldBe false
    trazas(settings.cargasDir, "estado").flatten.toSet shouldBe Set("error")
  }

  "SilverBuilder.startAll" should "keep a table pending until L2 has files for its source, then start it" in {
    val tmp = Files.createTempDirectory("silver-builder-pending-spec")
    val settings = settingsIn(tmp)
    val pendiente = Seq((fuente, tabla))

    SilverBuilder.startAll(pendiente, settings, hadoopConf) shouldBe pendiente   // ni existe l2/cnmc_indicadores
    spark.streams.active shouldBe empty

    escribirCnmc(settings, "2026-10-01", Seq(fila("2026T1", "Renfe Viajeros", "100"), fila("2026T1", "Iryo", "50")))
    SilverBuilder.startAll(pendiente, settings, hadoopConf) shouldBe empty
    spark.streams.active should have length 1
    spark.streams.active.foreach { q => q.processAllAvailable(); q.stop() }
    silver(settings, "cnmc_trimestral").count() shouldBe 2
  }

  "SilverBuilder.tablasDe" should "list every (source, table) pair declared in the sources" in {
    val sinSilver = DataSource("crtm", "CRTM", "https://example.invalid", "zip")

    SilverBuilder.tablasDe(Seq(sinSilver, fuente)) shouldBe Seq((fuente, tabla))
  }
}
