package raillytics.common.calidad

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, TimestampNTZType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.common.calidad.QualityGates.{Gate, QualityGateException}
import raillytics.testutil.{TestPaths, TestSpark}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files

case class Viajero(fecha: String, estacion_id: String, linea_id: String, viajeros: Long, condicion: String)
case class Linea(linea_id: String, nombre: Option[String])

class QualityGatesSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = TestSpark.session("QualityGatesSpec")
    spark.createDataFrame(Seq(
      Viajero("2025-04-17", "MADPA", "AVE-MAD-BCN", 1000L, "despejado"),
      Viajero("2025-04-17", "MADCH", "AVE-MAD-BCN", 500L, "lluvia"),
      Viajero("2025-04-17", "MADCH", "AVE-MAD-BCN", 700L, "granizo"),     // duplica el grano y sale del dominio
      Viajero("2025-04-18", "MADPA", "LD-MAD-SAN", -5L, "despejado")       // negativo y línea sin catálogo
    )).createOrReplaceTempView("t_viajeros")
    spark.createDataFrame(Seq(Linea("AVE-MAD-BCN", Some("AVE Madrid – Barcelona")), Linea("MD-MAD-TOL", None)))
      .createOrReplaceTempView("t_lineas")
  }

  override def afterAll(): Unit = spark.stop()

  private def yaml(texto: String) = new ByteArrayInputStream(texto.getBytes(StandardCharsets.UTF_8))

  "QualityGates.loadFromStream" should "parse every gate type keeping the file order and defaults" in {
    val gates = QualityGates.loadFromStream(yaml(
      """tablas:
        |  t_viajeros:
        |    - {nombre: filas, tipo: filas_min, minimo: 1}
        |    - {nombre: nulos, tipo: no_nulos, columnas: [fecha, viajeros], severidad: aviso}
        |    - {nombre: grano, tipo: unico, columnas: [fecha, estacion_id, linea_id]}
        |    - {nombre: dominio, tipo: dominio, columna: condicion, valores: [despejado, lluvia]}
        |    - {nombre: rango, tipo: rango, columna: viajeros, minimo: 0, maximo: 100000}
        |    - {nombre: ref, tipo: referencia, columnas: [linea_id], tabla: t_lineas}
        |    - {nombre: libre, tipo: sql, sql: "SELECT count(*) FROM t_viajeros WHERE viajeros > 900", maximo: 1}
        |  t_lineas:
        |    - {nombre: nombre, tipo: no_nulos, columnas: [nombre]}
        |""".stripMargin))

    gates.keys.toSeq shouldBe Seq("t_viajeros", "t_lineas")
    gates("t_viajeros").map(_.nombre) shouldBe Seq("filas", "nulos", "grano", "dominio", "rango", "ref", "libre")
    gates("t_viajeros").map(_.severidad) shouldBe Seq("bloqueante", "aviso", "bloqueante", "bloqueante", "bloqueante", "bloqueante", "bloqueante")
    val ref = gates("t_viajeros")(5)
    ref.columnas shouldBe Seq("linea_id")
    ref.columnasDestino shouldBe Seq("linea_id")
    ref.tabla shouldBe Some("t_lineas")
  }

  it should "reject a gate that does not carry what its type needs" in {
    an[IllegalArgumentException] should be thrownBy QualityGates.loadFromStream(yaml(
      "tablas:\n  t:\n    - {nombre: x, tipo: rango, columna: c}\n"))
    an[IllegalArgumentException] should be thrownBy QualityGates.loadFromStream(yaml(
      "tablas:\n  t:\n    - {nombre: x, tipo: inventado, columnas: [c]}\n"))
    an[IllegalArgumentException] should be thrownBy QualityGates.loadFromStream(yaml(
      "tablas:\n  t:\n    - {nombre: x, tipo: no_nulos, columnas: [c]}\n    - {nombre: x, tipo: unico, columnas: [c]}\n"))
  }

  it should "load the project quality_gates.yml" in {
    val gates = QualityGates.load("config/quality_gates.yml")
    gates.keys should contain allOf ("silver_viajeros_enriquecidos", "silver_puntualidad_enriquecida", "gold_dim_fecha",
      "gold_dim_estacion", "gold_dim_linea", "gold_fact_viajeros", "gold_fact_puntualidad")
    gates.values.flatten.map(_.nombre) should not be empty
  }

  "Gate.consulta" should "generate the same SQL for every type as the documented contract" in {
    Gate("g", "filas_min", "bloqueante", minimo = Some(1)).consulta("t") shouldBe "SELECT count(*) FROM t"
    Gate("g", "no_nulos", "bloqueante", columnas = Seq("a", "b")).consulta("t") shouldBe
      "SELECT count(*) FROM t WHERE a IS NULL OR b IS NULL"
    Gate("g", "unico", "bloqueante", columnas = Seq("a", "b")).consulta("t") shouldBe
      "SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) AS n FROM t GROUP BY a, b HAVING count(*) > 1) d"
    Gate("g", "dominio", "bloqueante", columnas = Seq("c"), valores = Seq("x", "o'k", 3)).consulta("t") shouldBe
      "SELECT count(*) FROM t WHERE c IS NOT NULL AND c NOT IN ('x', 'o''k', 3)"
    Gate("g", "rango", "bloqueante", columnas = Seq("c"), minimo = Some(0), maximo = Some(7.5)).consulta("t") shouldBe
      "SELECT count(*) FROM t WHERE c < 0 OR c > 7.5"
    Gate("g", "referencia", "bloqueante", columnas = Seq("a", "b"), tabla = Some("d"), columnasDestino = Seq("x", "y")).consulta("t") shouldBe
      "SELECT count(*) FROM t s WHERE s.a IS NOT NULL AND s.b IS NOT NULL AND NOT EXISTS (SELECT 1 FROM d d WHERE d.x = s.a AND d.y = s.b)"
    Gate("g", "sql", "bloqueante", sql = Some(" SELECT 1 ")).consulta("t") shouldBe "SELECT 1"
  }

  "QualityGates.evaluar" should "run every gate type with Spark SQL and report failures with their value" in {
    val gates = QualityGates.loadFromStream(yaml(
      """tablas:
        |  t_viajeros:
        |    - {nombre: filas, tipo: filas_min, minimo: 1}
        |    - {nombre: nulos, tipo: no_nulos, columnas: [fecha, viajeros]}
        |    - {nombre: grano, tipo: unico, columnas: [fecha, estacion_id, linea_id]}
        |    - {nombre: dominio, tipo: dominio, columna: condicion, valores: [despejado, lluvia], severidad: aviso}
        |    - {nombre: rango, tipo: rango, columna: viajeros, minimo: 0}
        |    - {nombre: ref, tipo: referencia, columnas: [linea_id], tabla: t_lineas}
        |    - {nombre: libre, tipo: sql, sql: "SELECT count(*) FROM t_viajeros WHERE viajeros > 900", maximo: 1}
        |    - {nombre: sin_columna, tipo: no_nulos, columnas: [no_existe]}
        |""".stripMargin))

    val res = QualityGates.evaluar("t_viajeros", gates("t_viajeros"))
    res.map(r => (r.gate, r.resultado, r.valor)) shouldBe Seq(
      ("filas", "ok", Some(4.0)),
      ("nulos", "ok", Some(0.0)),
      ("grano", "fallo", Some(1.0)),
      ("dominio", "fallo", Some(1.0)),
      ("rango", "fallo", Some(1.0)),
      ("ref", "fallo", Some(1.0)),
      ("libre", "ok", Some(1.0)),
      ("sin_columna", "error", None)
    )
    res.map(_.bloquea) shouldBe Seq(false, false, true, false, true, true, false, true)
    res.find(_.gate == "grano").get.detalle shouldBe Some("1 fila(s) duplicada(s) por fecha, estacion_id, linea_id")
    res.find(_.gate == "sin_columna").get.detalle.get should startWith("no se pudo evaluar")

    val thrown = the[QualityGateException] thrownBy QualityGates.exigir(res)
    thrown.fallidos.map(_.gate) shouldBe Seq("grano", "rango", "ref", "sin_columna")
    thrown.getMessage should include("t_viajeros.grano (valor=1, umbral='= 0')")
    noException should be thrownBy QualityGates.exigir(res.filterNot(_.bloquea))
  }

  it should "pass a clean table" in {
    val gates = Seq(
      Gate("clave", "unico", "bloqueante", columnas = Seq("linea_id")),
      Gate("nombre", "no_nulos", "aviso", columnas = Seq("nombre"))
    )
    val res = QualityGates.evaluar("t_lineas", gates)
    res.map(_.resultado) shouldBe Seq("ok", "fallo")
    res.exists(_.bloquea) shouldBe false
  }

  "QualityGates.registrar" should "write one row per result with the Python-compatible schema" in {
    val calidadDir = TestPaths.fileUri(Files.createTempDirectory("calidad-spec").resolve("calidad"))
    val res = QualityGates.evaluar("t_lineas", Seq(Gate("nombre", "no_nulos", "aviso", columnas = Seq("nombre")))) :+
      QualityGates.resultado("crtm", "bytes_subidos", QualityGates.Bloqueante, pasa = true, 6042, "= 6042")

    QualityGates.registrar(res, "20260925T100000-gold_build-abc123", "gold_build", "gold", calidadDir, lanzadoPor = "cli")

    val df = spark.read.parquet(calidadDir)
    df.schema.fieldNames shouldBe QualityGates.Schema.fieldNames
    df.schema("valor").dataType shouldBe DoubleType
    df.schema("inicio").dataType shouldBe TimestampNTZType
    val filas = df.orderBy("tabla").collect().map(_.toSeq)
    filas.map(f => (f(0), f(1), f(2), f(3), f(4), f(5), f(6), f(7), f(8), f(9))) shouldBe Array(
      ("20260925T100000-gold_build-abc123", "gold_build", "gold", "crtm", "bytes_subidos", "fichero", "bloqueante", "ok", 6042.0, "= 6042"),
      ("20260925T100000-gold_build-abc123", "gold_build", "gold", "t_lineas", "nombre", "no_nulos", "aviso", "fallo", 1.0, "= 0")
    )
    filas(1)(10) shouldBe "1 fila(s) con nulos en nombre"
    filas(0)(14) shouldBe "cli"
  }

  it should "not break the process when the record cannot be written" in {
    noException should be thrownBy QualityGates.registrar(
      Seq(QualityGates.resultado("t", "g", QualityGates.Aviso, pasa = true, 1, "= 1")),
      "run", "p", "bronze", "noexiste://bucket/calidad/")
  }
}
