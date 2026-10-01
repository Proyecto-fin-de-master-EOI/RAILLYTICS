package raillytics.common.calidad

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor
import raillytics.common.logging.Logging
import raillytics.common.trazabilidad.Cargas

import java.io.{FileInputStream, InputStream}
import java.net.InetAddress
import java.time.{LocalDateTime, ZoneOffset}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

// Quality Gates: el framework de control de calidad del lake, con Spark como motor.
//
// Un gate es una comprobación sobre una tabla (vista temporal de la sesión) que
// produce un valor numérico y lo compara con un umbral. Los gates declarativos
// viven en config/quality_gates.yml, por tabla (<capa>_<tabla>), y se traducen a
// una consulta Spark SQL; los procesos que no trabajan con tablas (L1/L2) crean
// resultados a mano con `resultado(...)` y los registran igual.
//
//   severidad bloqueante -> exigir() lanza QualityGateException y la carga no se promociona
//   severidad aviso      -> se registra y la carga sigue
//
// Cada evaluación deja una fila por gate en <trazabilidad>/calidad/ (Parquet, mismo
// esquema que python/raillytics/calidad/registro.py) con el run_id de la carga a la
// que pertenece, para cruzarla con la trazabilidad de cargas en Superset.
//
//   val gates = QualityGates.load("config/quality_gates.yml")
//   val res = QualityGates.evaluarTablas(gates, Seq("silver_viajeros_enriquecidos"))
//   QualityGates.registrar(res, ejecucion.runId, "gold_build", "silver", lake.calidadDir)
//   QualityGates.exigir(res)   // lanza si falla algún gate bloqueante
object QualityGates extends Logging {

  val Bloqueante = "bloqueante"
  val Aviso = "aviso"
  val Severidades: Set[String] = Set(Bloqueante, Aviso)

  val Ok = "ok"
  val Fallo = "fallo"
  val Error = "error"   // el gate no se pudo evaluar: cuenta como fallo (fail closed)

  val Tipos: Set[String] = Set("filas_min", "no_nulos", "unico", "dominio", "rango", "referencia", "sql")

  // Un gate declarativo. `columnas` vale tanto para los tipos de varias columnas
  // como para los de una (dominio, rango: la primera).
  final case class Gate(
    nombre: String,
    tipo: String,
    severidad: String,
    columnas: Seq[String] = Nil,
    valores: Seq[Any] = Nil,
    minimo: Option[Double] = None,
    maximo: Option[Double] = None,
    tabla: Option[String] = None,             // referencia: tabla destino
    columnasDestino: Seq[String] = Nil,       // referencia: columnas en la tabla destino (por defecto, las mismas)
    sql: Option[String] = None
  ) {
    require(Tipos.contains(tipo), s"gate '$nombre': tipo no soportado '$tipo' (soportados: ${Tipos.toSeq.sorted.mkString(", ")})")
    require(Severidades.contains(severidad), s"gate '$nombre': severidad no soportada '$severidad' (bloqueante | aviso)")
    tipo match {
      case "filas_min"  => require(minimo.isDefined, s"gate '$nombre' (filas_min): falta 'minimo'")
      case "no_nulos" | "unico" => require(columnas.nonEmpty, s"gate '$nombre' ($tipo): falta 'columnas'")
      case "dominio"    => require(columnas.size == 1 && valores.nonEmpty, s"gate '$nombre' (dominio): hacen falta 'columna' y 'valores'")
      case "rango"      => require(columnas.size == 1 && (minimo.isDefined || maximo.isDefined), s"gate '$nombre' (rango): hacen falta 'columna' y 'minimo' y/o 'maximo'")
      case "referencia" => require(columnas.nonEmpty && tabla.isDefined && columnasDestino.size == columnas.size,
                                   s"gate '$nombre' (referencia): hacen falta 'columnas' y 'tabla' (y 'columnas_destino' del mismo tamaño)")
      case "sql"        => require(sql.exists(_.trim.nonEmpty), s"gate '$nombre' (sql): falta 'sql'")
    }

    // Consulta Spark SQL que devuelve un único número: filas que infringen el
    // gate (0 = pasa) o, en filas_min y sql, la métrica que se compara con el umbral.
    def consulta(tablaObjetivo: String): String = tipo match {
      case "filas_min" =>
        s"SELECT count(*) FROM $tablaObjetivo"
      case "no_nulos" =>
        s"SELECT count(*) FROM $tablaObjetivo WHERE ${columnas.map(c => s"$c IS NULL").mkString(" OR ")}"
      case "unico" =>
        val cols = columnas.mkString(", ")
        s"SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) AS n FROM $tablaObjetivo GROUP BY $cols HAVING count(*) > 1) d"
      case "dominio" =>
        val c = columnas.head
        s"SELECT count(*) FROM $tablaObjetivo WHERE $c IS NOT NULL AND $c NOT IN (${valores.map(literal).mkString(", ")})"
      case "rango" =>
        val c = columnas.head
        val condiciones = minimo.map(m => s"$c < ${numero(m)}").toSeq ++ maximo.map(m => s"$c > ${numero(m)}").toSeq
        s"SELECT count(*) FROM $tablaObjetivo WHERE ${condiciones.mkString(" OR ")}"
      case "referencia" =>
        val noNulos = columnas.map(c => s"s.$c IS NOT NULL").mkString(" AND ")
        val join = columnas.zip(columnasDestino).map { case (c, d) => s"d.$d = s.$c" }.mkString(" AND ")
        s"SELECT count(*) FROM $tablaObjetivo s WHERE $noNulos AND NOT EXISTS (SELECT 1 FROM ${tabla.get} d WHERE $join)"
      case "sql" =>
        sql.get.trim
    }

    // Texto del umbral para el registro y los mensajes.
    def umbral: String = tipo match {
      case "filas_min" => s">= ${numero(minimo.get)}"
      case "sql" =>
        (minimo, maximo) match {
          case (Some(a), Some(b)) => s"entre ${numero(a)} y ${numero(b)}"
          case (Some(a), None)    => s">= ${numero(a)}"
          case (None, b)          => s"<= ${numero(b.getOrElse(0.0))}"
        }
      case _ => "= 0"
    }

    def pasa(valor: Double): Boolean = tipo match {
      case "filas_min" => valor >= minimo.get
      case "sql"       => minimo.forall(valor >= _) && valor <= maximo.getOrElse(0.0)
      case _           => valor == 0.0
    }
  }

  // Resultado de un gate (declarativo o programático) sobre una tabla o fichero.
  final case class Resultado(
    tabla: String,
    gate: String,
    tipo: String,
    severidad: String,
    resultado: String,
    valor: Option[Double],
    umbral: Option[String],
    detalle: Option[String],
    inicio: LocalDateTime,
    fin: LocalDateTime
  ) {
    def pasa: Boolean = resultado == Ok
    // Un fallo (o error de evaluación) bloqueante impide promocionar la carga.
    def bloquea: Boolean = !pasa && severidad == Bloqueante
  }

  // Al menos un gate bloqueante no ha pasado. Lleva todos los resultados de la evaluación.
  final class QualityGateException(val resultados: Seq[Resultado])
    extends RuntimeException(describirFallos(resultados.filter(_.bloquea))) {
    def fallidos: Seq[Resultado] = resultados.filter(_.bloquea)
  }

  def describirFallos(fallidos: Seq[Resultado]): String = {
    val partes = fallidos.map { r =>
      s"${r.tabla}.${r.gate}" +
        r.valor.map(v => s" (valor=${numero(v)}, umbral='${r.umbral.getOrElse("")}')").getOrElse("") +
        r.detalle.map(d => s": $d").getOrElse("")
    }
    s"${fallidos.size} quality gate(s) bloqueante(s) fallido(s): ${partes.mkString("; ")}"
  }

  // Mismo esquema que CALIDAD_COLUMNS en python/raillytics/calidad/registro.py.
  val Schema: StructType = StructType(Seq(
    StructField("run_id", StringType, nullable = false),
    StructField("proceso", StringType, nullable = false),
    StructField("capa", StringType, nullable = false),
    StructField("tabla", StringType, nullable = false),
    StructField("gate", StringType, nullable = false),
    StructField("tipo", StringType, nullable = false),
    StructField("severidad", StringType, nullable = false),
    StructField("resultado", StringType, nullable = false),
    StructField("valor", DoubleType),
    StructField("umbral", StringType),
    StructField("detalle", StringType),
    StructField("inicio", TimestampNTZType, nullable = false),
    StructField("fin", TimestampNTZType, nullable = false),
    StructField("duracion_s", DoubleType, nullable = false),
    StructField("lanzado_por", StringType, nullable = false),
    StructField("ejecutor", StringType, nullable = false),
    StructField("usuario", StringType)
  ))

  private def ahora(): LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)

  // ---------------------------------------------------------------- configuración

  def load(path: String): Map[String, Seq[Gate]] = {
    logger.info(s"cargando quality gates desde '$path'")
    val is = new FileInputStream(path)
    val gates = try loadFromStream(is) finally is.close()
    logger.info(s"${gates.values.map(_.size).sum} gate(s) en ${gates.size} tabla(s): ${gates.keys.toSeq.sorted.mkString(", ")}")
    gates
  }

  // Separado de load() para poder probarlo con un YAML en memoria. Conserva el
  // orden de tablas y gates del fichero.
  def loadFromStream(is: InputStream): Map[String, Seq[Gate]] = {
    val yaml = new Yaml(new SafeConstructor(new LoaderOptions()))
    val root = Option(yaml.load(is).asInstanceOf[java.util.Map[String, Object]])
      .getOrElse(throw new IllegalArgumentException("quality_gates.yml vacío"))
    val tablas = Option(root.get("tablas")).map(_.asInstanceOf[java.util.Map[String, Object]])
      .getOrElse(throw new IllegalArgumentException("quality_gates.yml: falta la clave raíz 'tablas'"))
    val resultado = scala.collection.immutable.ListMap.newBuilder[String, Seq[Gate]]
    tablas.asScala.foreach { case (tabla, lista) =>
      val entradas = Option(lista).map(_.asInstanceOf[java.util.List[java.util.Map[String, Object]]].asScala.toSeq).getOrElse(Nil)
      val gates = entradas.map(m => parseGate(tabla, m.asScala.toMap))
      val repetidos = gates.groupBy(_.nombre).collect { case (n, gs) if gs.size > 1 => n }
      require(repetidos.isEmpty, s"tabla '$tabla': gates con el mismo nombre: ${repetidos.mkString(", ")}")
      resultado += tabla -> gates
    }
    resultado.result()
  }

  private def parseGate(tabla: String, m: Map[String, Any]): Gate = {
    def str(k: String): Option[String] = m.get(k).flatMap(Option(_)).map(_.toString)
    def lista(k: String): Seq[Any] = m.get(k).flatMap(Option(_)) match {
      case Some(l: java.util.List[_]) => l.asScala.toSeq
      case Some(v)                    => Seq(v)
      case None                       => Nil
    }
    def num(k: String): Option[Double] = m.get(k).flatMap(Option(_)).map {
      case n: java.lang.Number => n.doubleValue()
      case other => throw new IllegalArgumentException(s"tabla '$tabla': '$k' debe ser numérico, no '$other'")
    }
    val nombre = str("nombre").getOrElse(throw new IllegalArgumentException(s"tabla '$tabla': un gate no tiene 'nombre'"))
    val columnas = lista("columnas").map(_.toString) match {
      case Nil => str("columna").toSeq
      case cs  => cs
    }
    val columnasDestino = lista("columnas_destino").map(_.toString) match {
      case Nil => columnas
      case cs  => cs
    }
    Gate(
      nombre = nombre,
      tipo = str("tipo").getOrElse(throw new IllegalArgumentException(s"tabla '$tabla', gate '$nombre': falta 'tipo'")),
      severidad = str("severidad").getOrElse(Bloqueante),
      columnas = columnas,
      valores = lista("valores"),
      minimo = num("minimo"),
      maximo = num("maximo"),
      tabla = str("tabla"),
      columnasDestino = columnasDestino,
      sql = str("sql")
    )
  }

  // ---------------------------------------------------------------- evaluación

  // Evalúa los gates de una tabla (ya registrada como vista en la sesión). Nunca
  // lanza: un gate que no se puede ejecutar (columna inexistente, vista sin
  // registrar...) queda con resultado 'error'.
  def evaluar(tabla: String, gates: Seq[Gate])(implicit spark: SparkSession): Seq[Resultado] =
    gates.map { gate =>
      val inicio = ahora()
      val consulta = gate.consulta(tabla)
      val r = try {
        val valor = aDouble(spark.sql(consulta).collect().headOption.map(_.get(0)).orNull)
        val pasa = gate.pasa(valor)
        Resultado(tabla, gate.nombre, gate.tipo, gate.severidad, if (pasa) Ok else Fallo, Some(valor), Some(gate.umbral),
                  if (pasa) None else Some(detalleFallo(gate, valor)), inicio, ahora())
      } catch {
        case NonFatal(e) =>
          Resultado(tabla, gate.nombre, gate.tipo, gate.severidad, Error, None, Some(gate.umbral),
                    Some(s"no se pudo evaluar: ${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("")}".take(2000)), inicio, ahora())
      }
      val nivel = if (r.pasa) "ok" else if (r.bloquea) "FALLO BLOQUEANTE" else "aviso"
      logger.info(s"gate $tabla.${gate.nombre} [$nivel] valor=${r.valor.map(numero).getOrElse("-")} umbral='${gate.umbral}'" +
        r.detalle.map(d => s" $d").getOrElse(""))
      r
    }

  // Evalúa varias tablas con los gates declarados para cada una (las que no tienen, nada).
  def evaluarTablas(gates: Map[String, Seq[Gate]], tablas: Seq[String])(implicit spark: SparkSession): Seq[Resultado] =
    tablas.flatMap(t => evaluar(t, gates.getOrElse(t, Nil)))

  // Lanza QualityGateException si algún gate bloqueante ha fallado o no se pudo evaluar.
  def exigir(resultados: Seq[Resultado]): Unit =
    if (resultados.exists(_.bloquea)) throw new QualityGateException(resultados)

  // Resultado de un gate programático (L1, L2: comprobaciones sobre ficheros que
  // no se expresan como SQL sobre una tabla).
  def resultado(tabla: String, gate: String, severidad: String, pasa: Boolean, valor: Double, umbral: String,
                detalle: Option[String] = None, tipo: String = "fichero", inicio: LocalDateTime = ahora()): Resultado =
    Resultado(tabla, gate, tipo, severidad, if (pasa) Ok else Fallo, Some(valor), Some(umbral), if (pasa) None else detalle, inicio, ahora())

  private def detalleFallo(gate: Gate, valor: Double): String = gate.tipo match {
    case "filas_min"  => s"${numero(valor)} fila(s), mínimo ${numero(gate.minimo.get)}"
    case "no_nulos"   => s"${numero(valor)} fila(s) con nulos en ${gate.columnas.mkString(", ")}"
    case "unico"      => s"${numero(valor)} fila(s) duplicada(s) por ${gate.columnas.mkString(", ")}"
    case "dominio"    => s"${numero(valor)} fila(s) con ${gate.columnas.head} fuera de ${gate.valores.mkString("[", ", ", "]")}"
    case "rango"      => s"${numero(valor)} fila(s) con ${gate.columnas.head} fuera de rango"
    case "referencia" => s"${numero(valor)} fila(s) sin correspondencia en ${gate.tabla.get} (${gate.columnas.mkString(", ")})"
    case "sql"        => s"valor ${numero(valor)}, umbral ${gate.umbral}"
  }

  private def aDouble(v: Any): Double = v match {
    case null                    => 0.0   // p. ej. sum() sin filas
    case n: java.lang.Number     => n.doubleValue()
    case b: java.lang.Boolean    => if (b) 1.0 else 0.0
    case d: scala.math.BigDecimal => d.toDouble
    case other => throw new IllegalArgumentException(s"la consulta del gate debe devolver un número, no '$other' (${other.getClass.getSimpleName})")
  }

  // Literal SQL (mismas reglas que el lado Python): cadenas entre comillas simples, el resto tal cual.
  private def literal(v: Any): String = v match {
    case s: String            => "'" + s.replace("'", "''") + "'"
    case n: java.lang.Number  => numero(n.doubleValue())
    case b: java.lang.Boolean => b.toString
    case other                => "'" + other.toString.replace("'", "''") + "'"
  }

  // Los enteros sin ".0" para que el SQL y los mensajes queden naturales (viajeros < 0, no 0.0).
  def numero(d: Double): String = if (d == d.toLong.toDouble && !d.isInfinite) d.toLong.toString else d.toString

  // ---------------------------------------------------------------- registro

  // Filas en el orden de Schema.
  def filasRegistro(resultados: Seq[Resultado], runId: String, proceso: String, capa: String, lanzadoPor: String): Seq[Row] = {
    val ejecutor = Try(InetAddress.getLocalHost.getHostName).getOrElse("desconocido")
    val usuario = sys.props.get("user.name").orNull
    resultados.map { r =>
      Row(
        runId, proceso, capa, r.tabla, r.gate, r.tipo, r.severidad, r.resultado,
        r.valor.map(Double.box).orNull, r.umbral.orNull, r.detalle.map(_.take(2000)).orNull,
        r.inicio, r.fin, java.time.Duration.between(r.inicio, r.fin).toNanos / 1e9,
        lanzadoPor, ejecutor, usuario
      )
    }
  }

  // Un fichero Parquet por evaluación, añadido al directorio compartido con el lado
  // Python. Best effort, como la trazabilidad de cargas: si no se puede escribir
  // queda en el log, pero el resultado de los gates ya se ha aplicado a la carga.
  def registrar(resultados: Seq[Resultado], runId: String, proceso: String, capa: String, calidadDir: String,
                lanzadoPor: String = Cargas.lanzadoPorDefecto())(implicit spark: SparkSession): Unit =
    if (resultados.nonEmpty) {
      try {
        spark.createDataFrame(filasRegistro(resultados, runId, proceso, capa, lanzadoPor).asJava, Schema)
          .coalesce(1)
          .write.mode("append").parquet(calidadDir)
        logger.info(s"${resultados.size} resultado(s) de quality gates de $runId registrados en $calidadDir")
      } catch {
        case NonFatal(e) => logger.error(s"no se pudieron registrar los resultados de quality gates de $runId", e)
      }
    }

  // Resumen legible para la consola de las apps.
  def resumen(resultados: Seq[Resultado]): String = {
    val ok = resultados.count(_.pasa)
    val bloqueantes = resultados.count(_.bloquea)
    val avisos = resultados.count(r => !r.pasa && !r.bloquea)
    val lineas = resultados.filterNot(_.pasa).map { r =>
      f"  ${if (r.bloquea) "FALLO" else "aviso"}%-6s ${r.tabla}.${r.gate}: ${r.detalle.getOrElse("")}"
    }
    (s"Quality gates: $ok ok, $bloqueantes fallo(s) bloqueante(s), $avisos aviso(s)" +: lineas).mkString("\n")
  }

  // Acumulador cómodo para los procesos que generan resultados programáticos.
  final class Acumulador {
    private val buf = ListBuffer.empty[Resultado]
    def +=(r: Resultado): Resultado = { buf += r; r }
    def resultados: Seq[Resultado] = buf.toSeq
  }
}
