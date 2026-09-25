package raillytics.common.trazabilidad

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import raillytics.common.logging.Logging

import java.net.InetAddress
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

// Trazabilidad de cargas: el equivalente Scala de python/raillytics/utils/cargas.py.
// Cada ejecución de un proceso de carga deja una fila por tabla cargada en
// <gold>/_trazabilidad/cargas/ (Parquet con las mismas columnas y tipos que el
// lado Python). Spark añade sus part-*.parquet al directorio (mode append) y
// DuckDB/Superset los leen junto a los ficheros que escribe Python.
//
//   Cargas.registrar("gold_build", "gold", cargasDir, Map("umbral" -> 5)) { ejecucion =>
//     ejecucion.tabla("dim_fecha", origen = Some(silverRoot), destino = Some(dest)) { carga =>
//       ...                        // la carga propiamente dicha
//       carga.filas = Some(365)
//     }
//   }
//
// El registro se escribe al salir del bloque exterior, también cuando hay un
// error (la excepción se propaga). Es "best effort": si no se puede escribir
// queda constancia en el log, pero la carga no falla por eso.
object Cargas extends Logging {

  val EstadoOk = "ok"
  val EstadoError = "error"

  // Mismo esquema que CARGAS_COLUMNS en Python. TimestampNTZ se guarda en
  // Parquet como TIMESTAMP sin zona horaria, que es como DuckDB escribe
  // inicio/fin (UTC por convenio); así los ficheros de ambos lados encajan.
  val Schema: StructType = StructType(Seq(
    StructField("run_id", StringType, nullable = false),
    StructField("proceso", StringType, nullable = false),
    StructField("capa", StringType, nullable = false),
    StructField("tabla", StringType),
    StructField("origen", StringType),
    StructField("destino", StringType),
    StructField("filas", LongType),
    StructField("bytes", LongType),
    StructField("inicio", TimestampNTZType, nullable = false),
    StructField("fin", TimestampNTZType, nullable = false),
    StructField("duracion_s", DoubleType, nullable = false),
    StructField("estado", StringType, nullable = false),
    StructField("error", StringType),
    StructField("parametros", StringType),
    StructField("lanzado_por", StringType, nullable = false),
    StructField("ejecutor", StringType, nullable = false),
    StructField("usuario", StringType)
  ))

  private val RunIdFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")
  private val Json = new ObjectMapper()

  def ahora(): LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)

  def lanzadoPorDefecto(env: Map[String, String] = sys.env): String =
    env.get("AIRFLOW_CTX_DAG_ID").map(dag => s"airflow:$dag")
      .orElse(if (env.contains("MAKELEVEL")) Some("make") else None)  // GNU make lo exporta a sus subprocesos
      .getOrElse("cli")

  private def describir(e: Throwable): String =
    s"${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("")}".take(2000)

  // Carga de una tabla dentro de una ejecución; los campos se rellenan en el bloque.
  final class CargaTabla(val tabla: Option[String], var origen: Option[String], var destino: Option[String],
                         val inicio: LocalDateTime = ahora()) {
    var filas: Option[Long] = None
    var bytes: Option[Long] = None
    var fin: Option[LocalDateTime] = None
    var estado: String = EstadoOk
    var error: Option[String] = None
  }

  // Una ejecución de un proceso de carga y las tablas que ha cargado.
  final class Ejecucion(val proceso: String, val capa: String, val parametros: Map[String, Any], val lanzadoPor: String) {
    val inicio: LocalDateTime = ahora()
    val runId: String = s"${inicio.format(RunIdFormatter)}-$proceso-${UUID.randomUUID().toString.take(6)}"
    private val tablas = ListBuffer.empty[CargaTabla]

    def tabla[T](nombre: String, origen: Option[String] = None, destino: Option[String] = None)
                (bloque: CargaTabla => T): T = {
      val carga = new CargaTabla(Some(nombre), origen, destino)
      tablas += carga
      try bloque(carga)
      catch {
        case NonFatal(e) =>
          carga.estado = EstadoError
          carga.error = Some(describir(e))
          throw e
      } finally carga.fin = Some(ahora())
    }

    // Error fuera de cualquier tabla (p. ej. antes de la primera): fila de ejecución sin tabla.
    def marcarError(e: Throwable): Unit =
      if (!tablas.exists(_.estado == EstadoError)) {
        val carga = new CargaTabla(None, None, None, inicio)
        carga.fin = Some(ahora())
        carga.estado = EstadoError
        carga.error = Some(describir(e))
        tablas += carga
      }

    // Filas en el orden de Schema; una ejecución sin tablas deja una fila sin tabla.
    def filasRegistro(): Seq[Row] = {
      val fin = ahora()
      val registradas = if (tablas.isEmpty) Seq(new CargaTabla(None, None, None, inicio)) else tablas.toSeq
      val parametrosJson = if (parametros.isEmpty) null else Json.writeValueAsString(aJava(parametros))
      val ejecutor = Try(InetAddress.getLocalHost.getHostName).getOrElse("desconocido")
      val usuario = sys.props.get("user.name").orNull
      registradas.map { t =>
        val tFin = t.fin.getOrElse(fin)
        Row(
          runId, proceso, capa, t.tabla.orNull, t.origen.orNull, t.destino.orNull,
          t.filas.map(Long.box).orNull, t.bytes.map(Long.box).orNull,
          t.inicio, tFin, java.time.Duration.between(t.inicio, tFin).toNanos / 1e9,
          t.estado, t.error.orNull, parametrosJson, lanzadoPor, ejecutor, usuario
        )
      }
    }
  }

  // Jackson serializa colecciones Java, no Scala: se convierten antes de volcar a JSON.
  private def aJava(value: Any): Any = value match {
    case m: Map[_, _]      => m.map { case (k, v) => k.toString -> aJava(v) }.asJava
    case it: Iterable[_]   => it.map(aJava).toSeq.asJava
    case other             => other
  }

  def registrar[T](proceso: String, capa: String, cargasDir: String, parametros: Map[String, Any] = Map.empty,
                   lanzadoPor: String = lanzadoPorDefecto())
                  (bloque: Ejecucion => T)(implicit spark: SparkSession): T = {
    val ejecucion = new Ejecucion(proceso, capa, parametros, lanzadoPor)
    try bloque(ejecucion)
    catch {
      case NonFatal(e) =>
        ejecucion.marcarError(e)
        throw e
    } finally escribirBestEffort(ejecucion, cargasDir)
  }

  // Un único fichero Parquet por ejecución, añadido al directorio compartido.
  def escribir(ejecucion: Ejecucion, cargasDir: String)(implicit spark: SparkSession): Unit =
    spark.createDataFrame(ejecucion.filasRegistro().asJava, Schema)
      .coalesce(1)
      .write.mode("append").parquet(cargasDir)

  private def escribirBestEffort(ejecucion: Ejecucion, cargasDir: String)(implicit spark: SparkSession): Unit =
    try {
      escribir(ejecucion, cargasDir)
      logger.info(s"carga ${ejecucion.runId} registrada en $cargasDir")
    } catch {
      case NonFatal(e) => logger.error(s"no se pudo registrar la trazabilidad de la carga ${ejecucion.runId}", e)
    }
}
