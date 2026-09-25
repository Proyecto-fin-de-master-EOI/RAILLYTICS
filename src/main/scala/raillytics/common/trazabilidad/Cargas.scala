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

  // Valores de la columna estado (los mismos que en Python).
  private val EstadoOk = "ok"
  private val EstadoError = "error"

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

  // Prefijo del run_id: instante de inicio en UTC, legible y ordenable.
  private val RunIdFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")
  // Serializa `parametros` a JSON; Jackson ya viene en el classpath de Spark.
  private val Json = new ObjectMapper()

  // Instante actual en UTC y sin zona horaria: es el convenio de inicio/fin
  // (el dashboard de Superset los pasa a hora de Madrid al mostrarlos).
  private def ahora(): LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)

  // Quién lanzó la carga, deducido del entorno: una tarea de Airflow (que
  // exporta su dag_id), `make` o la línea de comandos. Igual que en Python.
  def lanzadoPorDefecto(env: Map[String, String] = sys.env): String =
    env.get("AIRFLOW_CTX_DAG_ID").map(dag => s"airflow:$dag")
      .orElse(if (env.contains("MAKELEVEL")) Some("make") else None)  // GNU make lo exporta a sus subprocesos
      .getOrElse("cli")

  // Tipo y mensaje de la excepción para la columna error, acotado en tamaño.
  private def describir(e: Throwable): String =
    s"${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("")}".take(2000)

  // Carga de una tabla dentro de una ejecución; los campos se rellenan en el bloque.
  // tabla es None solo en la fila "de ejecución" que deja un error fuera de las tablas.
  final class CargaTabla(val tabla: Option[String], var origen: Option[String], var destino: Option[String],
                         val inicio: LocalDateTime = ahora()) {
    var filas: Option[Long] = None            // registros escritos (None si no aplica, p. ej. un fichero en bruto)
    var bytes: Option[Long] = None            // tamaño en bytes cuando se conoce (ficheros de L1)
    var fin: Option[LocalDateTime] = None     // lo fija tabla() al salir del bloque, con o sin error
    var estado: String = EstadoOk
    var error: Option[String] = None
  }

  // Una ejecución de un proceso de carga y las tablas que ha cargado.
  final class Ejecucion(val proceso: String, val capa: String, val parametros: Map[String, Any], val lanzadoPor: String) {
    val inicio: LocalDateTime = ahora()
    // <AAAAMMDDTHHMMSS>-<proceso>-<6 hex>: único, y ordenable por fecha en cualquier listado.
    val runId: String = s"${inicio.format(RunIdFormatter)}-$proceso-${UUID.randomUUID().toString.take(6)}"
    private val tablas = ListBuffer.empty[CargaTabla]

    // Ejecuta `bloque` como la carga de la tabla `nombre`: si lanza, la fila
    // queda en estado error con el mensaje y la excepción sigue su curso; fin
    // se fija siempre. Devuelve lo que devuelva el bloque.
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
    // ejecutor es el host y usuario quien corre el proceso: permiten saber desde
    // qué máquina y cuenta se hizo cada carga (make en un portátil, contenedor...).
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

  // Registra una ejecución del proceso `proceso` (capa bronze/silver/gold) mientras
  // corre `bloque`. cargasDir es el directorio del registro (LakeSettings.cargasDir) y
  // parametros lo que haga falta para reproducir la carga (umbrales, fuente...).
  // Devuelve lo que devuelva el bloque; si el bloque lanza, se registra y se relanza.
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

  // Un único fichero Parquet por ejecución, añadido al directorio compartido
  // (mode append: Spark le da un nombre único, así dos ejecuciones concurrentes
  // no se pisan y en S3 no hace falta reescribir nada).
  private def escribir(ejecucion: Ejecucion, cargasDir: String)(implicit spark: SparkSession): Unit =
    spark.createDataFrame(ejecucion.filasRegistro().asJava, Schema)
      .coalesce(1)
      .write.mode("append").parquet(cargasDir)

  // La trazabilidad nunca hace fallar la carga: si no se puede escribir (MinIO
  // caído, ruta inválida...), queda en el log con la excepción y se sigue.
  private def escribirBestEffort(ejecucion: Ejecucion, cargasDir: String)(implicit spark: SparkSession): Unit =
    try {
      escribir(ejecucion, cargasDir)
      logger.info(s"carga ${ejecucion.runId} registrada en $cargasDir")
    } catch {
      case NonFatal(e) => logger.error(s"no se pudo registrar la trazabilidad de la carga ${ejecucion.runId}", e)
    }
}
