package raillytics.common.trazabilidad

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, TimestampNTZType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.testutil.TestPaths

import java.nio.file.Files
import java.time.LocalDateTime

class CargasSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("CargasSpec").master("local[1]").getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  private def nuevoDir(): String = TestPaths.fileUri(Files.createTempDirectory("cargas-spec").resolve("cargas"))

  // Filas como Seq[Any] en el orden de Cargas.Schema, ordenadas por inicio y tabla.
  private def leer(cargasDir: String): Array[Seq[Any]] =
    spark.read.parquet(cargasDir).select(Cargas.Schema.fieldNames.map(org.apache.spark.sql.functions.col): _*)
      .orderBy("inicio", "tabla").collect().map(_.toSeq)

  "Cargas.registrar" should "write one row per table with the Python-compatible schema" in {
    val cargasDir = nuevoDir()

    val ejecucion = Cargas.registrar("gold_build", "gold", cargasDir, Map("umbral" -> 5, "tablas" -> Seq("a", "b")),
                                     lanzadoPor = "cli") { ejecucion =>
      ejecucion.tabla("dim_fecha", origen = Some("silver"), destino = Some("gold/dim_fecha/")) { carga =>
        carga.filas = Some(365L)
      }
      ejecucion.tabla("fact_viajeros", origen = Some("silver"), destino = Some("gold/fact_viajeros/")) { carga =>
        carga.filas = Some(15330L)
        carga.bytes = Some(1024L)
      }
      ejecucion
    }

    val df = spark.read.parquet(cargasDir)
    df.schema.fieldNames shouldBe Cargas.Schema.fieldNames
    df.schema("inicio").dataType shouldBe TimestampNTZType
    df.schema("filas").dataType shouldBe LongType

    val filas = leer(cargasDir)
    filas.map(_(3)) shouldBe Array("dim_fecha", "fact_viajeros")
    val Seq(runId, proceso, capa, _, origen, destino, n, nBytes, inicio, fin, duracion, estado, error, parametros,
            lanzado, ejecutor, usuario) = filas(0)
    runId shouldBe ejecucion.runId
    runId.asInstanceOf[String].split("-")(1) shouldBe "gold_build"
    Seq(proceso, capa, origen, destino, n, nBytes) shouldBe Seq("gold_build", "gold", "silver", "gold/dim_fecha/", 365L, null)
    Seq(estado, error, lanzado) shouldBe Seq("ok", null, "cli")
    parametros shouldBe """{"umbral":5,"tablas":["a","b"]}"""
    fin.asInstanceOf[LocalDateTime].isBefore(inicio.asInstanceOf[LocalDateTime]) shouldBe false
    duracion.asInstanceOf[Double] should be >= 0.0
    filas(1)(7) shouldBe 1024L
    ejecutor.asInstanceOf[String] should not be empty
    usuario.asInstanceOf[String] should not be empty
  }

  it should "record an error inside a table, keep the others and rethrow" in {
    val cargasDir = nuevoDir()

    val thrown = the[IllegalStateException] thrownBy {
      Cargas.registrar("gold_build", "gold", cargasDir) { ejecucion =>
        ejecucion.tabla("dim_fecha") { carga => carga.filas = Some(10L) }
        ejecucion.tabla("dim_linea") { _ => throw new IllegalStateException("parquet corrupto") }
      }
    }
    thrown.getMessage shouldBe "parquet corrupto"

    val filas = leer(cargasDir)
    filas.map(fila => (fila(3), fila(11), fila(6))) shouldBe Array(("dim_fecha", "ok", 10L), ("dim_linea", "error", null))
    filas(1)(12) shouldBe "IllegalStateException: parquet corrupto"
  }

  it should "leave a run-level error row when the failure happens outside any table" in {
    val cargasDir = nuevoDir()

    a[RuntimeException] should be thrownBy {
      Cargas.registrar("silver_job", "silver", cargasDir) { _ => throw new RuntimeException("MinIO no responde") }
    }

    val filas = leer(cargasDir)
    filas should have length 1
    (filas(0)(3), filas(0)(11), filas(0)(12)) shouldBe ((null, "error", "RuntimeException: MinIO no responde"))
  }

  it should "not break the load when the record itself cannot be written" in {
    // Esquema desconocido para Hadoop: la escritura del registro falla, la carga no.
    val resultado = Cargas.registrar("gold_build", "gold", "noexiste://bucket/cargas/") { ejecucion =>
      ejecucion.tabla("dim_fecha") { carga => carga.filas = Some(1L) }
      "ok"
    }
    resultado shouldBe "ok"
  }

  "Cargas.lanzadoPorDefecto" should "detect make and Airflow from the environment" in {
    Cargas.lanzadoPorDefecto(Map.empty) shouldBe "cli"
    Cargas.lanzadoPorDefecto(Map("MAKELEVEL" -> "1")) shouldBe "make"
    Cargas.lanzadoPorDefecto(Map("AIRFLOW_CTX_DAG_ID" -> "ingesta", "MAKELEVEL" -> "1")) shouldBe "airflow:ingesta"
  }
}
