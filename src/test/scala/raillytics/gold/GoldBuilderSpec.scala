package raillytics.gold

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.common.calidad.QualityGates
import raillytics.common.lake.LakeSettings
import raillytics.testutil.{TestPaths, TestSpark}

import java.nio.file.{Files, Path}
import java.time.{LocalDate, LocalDateTime}
import scala.jdk.CollectionConverters._

// Filas Silver con el contrato de columnas de raillytics/procesamiento/silver_sample.py.
case class SilverViajeros(
  fecha: LocalDate, estacion_id: String, estacion_nombre: String, provincia: String, comunidad: String,
  latitud: Double, longitud: Double, linea_id: String, linea_nombre: String, tipo_tren: String,
  origen: String, destino: String, viajeros: Long, temperatura_media: Double, precipitacion_mm: Double,
  condicion_meteo: String, es_festivo: Boolean, festivo_nombre: Option[String]
)

case class SilverPuntualidad(
  fecha: LocalDate, servicio_id: String, linea_id: String, estacion_id: String,
  hora_prevista: LocalDateTime, hora_real: Option[LocalDateTime], retraso_min: Option[Int], estado: String,
  temperatura_media: Double, precipitacion_mm: Double, condicion_meteo: String, es_festivo: Boolean
)

class GoldBuilderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private implicit var spark: SparkSession = _
  private var tmpDir: Path = _
  private var settings: GoldBuilderSettings = _
  private var counts: Map[String, Long] = _

  private val jueves = LocalDate.of(2025, 4, 17)
  private val viernesSanto = LocalDate.of(2025, 4, 18)

  override def beforeAll(): Unit = {
    spark = TestSpark.session("GoldBuilderSpec")
    tmpDir = Files.createTempDirectory("gold-builder-spec")
    settings = settingsIn(tmpDir)
    escribirSilver(settings, filasPuntualidad)
    counts = GoldBuilder.build(settings)
  }

  override def afterAll(): Unit = spark.stop()

  private def settingsIn(dir: Path): GoldBuilderSettings = GoldBuilderSettings(
    lake = LakeSettings(
      bronzeRoot = TestPaths.fileUri(dir.resolve("bronze")),
      silverRoot = TestPaths.fileUri(dir.resolve("silver")),
      goldRoot = TestPaths.fileUri(dir.resolve("gold")),
      trazabilidadRoot = TestPaths.fileUri(dir.resolve("traza"))
    ),
    umbralPuntualidadMin = 5,
    calidadConfig = "config/quality_gates.yml"   // el YAML real del proyecto, desde la raíz del repo
  )

  private def viajeros(fecha: LocalDate, estacion: (String, String), n: Long, festivo: Option[String]) =
    SilverViajeros(fecha, estacion._1, estacion._2, "Madrid", "Comunidad de Madrid", 40.4, -3.7,
      "AVE-MAD-BCN", "AVE Madrid – Barcelona", "AVE", "Madrid Puerta de Atocha", "Barcelona Sants",
      n, 18.5, 0.0, "despejado", festivo.isDefined, festivo)

  private val atocha = ("MADPA", "Madrid Puerta de Atocha")
  private val chamartin = ("MADCH", "Madrid Chamartín")
  private val prevista = LocalDateTime.of(2025, 4, 18, 9, 30)
  private val filasPuntualidad = Seq(
    SilverPuntualidad(viernesSanto, "s1", "AVE-MAD-BCN", "MADPA", prevista, Some(prevista.plusMinutes(3)), Some(3), "realizado", 18.5, 0.0, "despejado", true),
    SilverPuntualidad(viernesSanto, "s2", "AVE-MAD-BCN", "MADPA", prevista.plusHours(2), Some(prevista.plusHours(2).plusMinutes(12)), Some(12), "realizado", 18.5, 0.0, "despejado", true),
    SilverPuntualidad(viernesSanto, "s3", "AVE-MAD-BCN", "MADCH", prevista.plusHours(4), None, None, "cancelado", 18.5, 0.0, "despejado", true)
  )

  private def escribirSilver(settings: GoldBuilderSettings, puntualidad: Seq[SilverPuntualidad]): Unit = {
    val filasViajeros = Seq(
      viajeros(jueves, atocha, 1000L, None), viajeros(jueves, chamartin, 500L, None),
      viajeros(viernesSanto, atocha, 1500L, Some("Viernes Santo")), viajeros(viernesSanto, chamartin, 700L, Some("Viernes Santo"))
    )
    spark.createDataFrame(filasViajeros).write.parquet(settings.lake.silverTable("viajeros_enriquecidos"))
    spark.createDataFrame(puntualidad).write.parquet(settings.lake.silverTable("puntualidad_enriquecida"))
  }

  private def gold(table: String) = spark.read.parquet(settings.lake.goldTable(table))

  "GoldBuilder.build" should "write every Gold table as a single parquet file and report its rows" in {
    counts shouldBe Map("dim_fecha" -> 2L, "dim_estacion" -> 2L, "dim_linea" -> 1L, "fact_viajeros" -> 4L, "fact_puntualidad" -> 3L)
    GoldBuilder.GoldTables.foreach { table =>
      val dir = tmpDir.resolve(s"gold/$table")
      Files.list(dir).iterator().asScala.count(_.toString.endsWith(".parquet")) shouldBe 1
      gold(table).count() shouldBe counts(table)
    }
  }

  it should "derive the calendar attributes of dim_fecha" in {
    val fila = gold("dim_fecha").filter(s"fecha = DATE '$viernesSanto'")
      .select("fecha_id", "anio", "trimestre", "mes", "nombre_mes", "dia_semana", "nombre_dia", "es_fin_de_semana",
              "es_festivo", "festivo_nombre", "estacion_anio")
      .collect().head.toSeq
    fila shouldBe Seq(20250418, 2025, 2, 4, "abril", 5, "viernes", false, true, "Viernes Santo", "primavera")

    gold("dim_fecha").filter(s"fecha = DATE '$jueves'").select("es_festivo", "festivo_nombre").collect().head.toSeq shouldBe Seq(false, null)
  }

  it should "resolve fact_viajeros against the dimensions" in {
    gold("fact_viajeros").join(gold("dim_fecha"), Seq("fecha_id"), "left_anti").count() shouldBe 0
    gold("fact_viajeros").join(gold("dim_estacion"), Seq("estacion_id"), "left_anti").count() shouldBe 0
    gold("fact_viajeros").agg(sum("viajeros")).collect().head.getLong(0) shouldBe 3700L
    gold("dim_linea").select("nombre", "tipo_tren", "origen", "destino").collect().head.toSeq shouldBe
      Seq("AVE Madrid – Barcelona", "AVE", "Madrid Puerta de Atocha", "Barcelona Sants")
  }

  it should "flag punctuality and cancellations in fact_puntualidad" in {
    val filas = gold("fact_puntualidad").orderBy("servicio_id")
      .select("servicio_id", "hora", "retraso_min", "es_puntual", "cancelado", "hora_real")
      .collect().map(_.toSeq)
    filas.map(f => (f(0), f(1), f(3), f(4))) shouldBe Array(("s1", 9, true, false), ("s2", 11, false, false), ("s3", 13, false, true))
    Option(filas(2)(2)) shouldBe None  // retraso_min del cancelado
    Option(filas(2)(5)) shouldBe None  // hora_real del cancelado
  }

  it should "leave one traceability row per Gold table" in {
    val trazas = spark.read.parquet(settings.lake.cargasDir)
      .select("proceso", "capa", "tabla", "filas", "estado", "parametros")
      .collect().map(_.toSeq)
    trazas.map(t => (t(2), t(3), t(4))).toSet shouldBe GoldBuilder.GoldTables.map(table => (table, counts(table), "ok")).toSet
    trazas.map(_(0)).toSet shouldBe Set("gold_build")
    trazas.head(5).asInstanceOf[String] should include (""""umbral_puntualidad_min":5""")
  }

  it should "record the Silver (entrada) and Gold (salida) quality gates under the same run_id, all passing" in {
    val runId = spark.read.parquet(settings.lake.cargasDir).select("run_id").distinct().collect().map(_.getString(0)).toSeq
    runId should have length 1
    val calidad = spark.read.parquet(settings.lake.calidadDir).select("run_id", "proceso", "capa", "tabla", "gate", "resultado").collect().map(_.toSeq)
    calidad.map(_(0)).toSet shouldBe runId.toSet
    calidad.map(_(1)).toSet shouldBe Set("gold_build")
    calidad.map(_(2)).toSet shouldBe Set("silver", "gold")
    calidad.map(_(3)).toSet shouldBe Set("silver_viajeros_enriquecidos", "silver_puntualidad_enriquecida") ++ GoldBuilder.GoldTables.map("gold_" + _)
    calidad.filterNot(_(5) == "ok") shouldBe empty
  }

  it should "abort before writing anything when a blocking quality gate fails" in {
    val dir = Files.createTempDirectory("gold-builder-gate-spec")
    val ajustes = settingsIn(dir)
    // Un servicio de una línea que no existe en viajeros: dim_linea no la tendría y el
    // dashboard perdería el servicio. El gate silver_puntualidad_enriquecida.lineas_en_viajeros
    // (bloqueante) debe parar la construcción antes de escribir Gold.
    val huerfano = SilverPuntualidad(viernesSanto, "s9", "LD-NO-EXISTE", "MADPA", prevista, Some(prevista), Some(0), "realizado", 18.5, 0.0, "despejado", true)
    escribirSilver(ajustes, filasPuntualidad :+ huerfano)

    val thrown = the[QualityGates.QualityGateException] thrownBy GoldBuilder.build(ajustes)

    thrown.fallidos.map(r => (r.tabla, r.gate)) shouldBe Seq(("silver_puntualidad_enriquecida", "lineas_en_viajeros"))
    Files.exists(dir.resolve("gold")) shouldBe false
    val carga = spark.read.parquet(ajustes.lake.cargasDir).select("tabla", "estado", "error").collect().map(_.toSeq)
    carga should have length 1
    Option(carga.head(0)) shouldBe None   // fila de ejecución, sin tabla
    carga.head(1) shouldBe "error"
    carga.head(2).asInstanceOf[String] should include("lineas_en_viajeros")
    val calidad = spark.read.parquet(ajustes.lake.calidadDir).select("capa", "gate", "resultado").collect().map(_.toSeq)
    calidad.map(_(0)).toSet shouldBe Set("silver")   // los gates de Gold nunca llegaron a evaluarse
    calidad.filter(_(2) == "fallo").map(_(1)) shouldBe Array("lineas_en_viajeros")
  }
}
