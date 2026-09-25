package raillytics.gold

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
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
    settings = GoldBuilderSettings(
      lake = LakeSettings(
        bronzeRoot = TestPaths.fileUri(tmpDir.resolve("bronze")),
        silverRoot = TestPaths.fileUri(tmpDir.resolve("silver")),
        goldRoot = TestPaths.fileUri(tmpDir.resolve("gold")),
        trazabilidadRoot = TestPaths.fileUri(tmpDir.resolve("traza"))
      ),
      umbralPuntualidadMin = 5
    )
    escribirSilver()
    counts = GoldBuilder.build(settings)
  }

  override def afterAll(): Unit = spark.stop()

  private def viajeros(fecha: LocalDate, estacion: (String, String), n: Long, festivo: Option[String]) =
    SilverViajeros(fecha, estacion._1, estacion._2, "Madrid", "Comunidad de Madrid", 40.4, -3.7,
      "AVE-MAD-BCN", "AVE Madrid – Barcelona", "AVE", "Madrid Puerta de Atocha", "Barcelona Sants",
      n, 18.5, 0.0, "despejado", festivo.isDefined, festivo)

  private def escribirSilver(): Unit = {
    val atocha = ("MADPA", "Madrid Puerta de Atocha")
    val chamartin = ("MADCH", "Madrid Chamartín")
    val filasViajeros = Seq(
      viajeros(jueves, atocha, 1000L, None), viajeros(jueves, chamartin, 500L, None),
      viajeros(viernesSanto, atocha, 1500L, Some("Viernes Santo")), viajeros(viernesSanto, chamartin, 700L, Some("Viernes Santo"))
    )
    val prevista = LocalDateTime.of(2025, 4, 18, 9, 30)
    val filasPuntualidad = Seq(
      SilverPuntualidad(viernesSanto, "s1", "AVE-MAD-BCN", "MADPA", prevista, Some(prevista.plusMinutes(3)), Some(3), "realizado", 18.5, 0.0, "despejado", true),
      SilverPuntualidad(viernesSanto, "s2", "AVE-MAD-BCN", "MADPA", prevista.plusHours(2), Some(prevista.plusHours(2).plusMinutes(12)), Some(12), "realizado", 18.5, 0.0, "despejado", true),
      SilverPuntualidad(viernesSanto, "s3", "AVE-MAD-BCN", "MADCH", prevista.plusHours(4), None, None, "cancelado", 18.5, 0.0, "despejado", true)
    )
    spark.createDataFrame(filasViajeros).write.parquet(settings.lake.silverTable("viajeros_enriquecidos"))
    spark.createDataFrame(filasPuntualidad).write.parquet(settings.lake.silverTable("puntualidad_enriquecida"))
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
}
