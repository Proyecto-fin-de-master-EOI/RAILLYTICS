package raillytics.silver

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, input_file_name, lit, regexp_extract}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import raillytics.common.calidad.QualityGates
import raillytics.common.lake.LakeViews
import raillytics.common.logging.Logging
import raillytics.common.trazabilidad.Cargas
import raillytics.ingesta.config.{DataSource, SilverTabla}

import java.nio.charset.StandardCharsets.UTF_8
import java.time.LocalDate
import scala.util.{Failure, Success, Try}

// Silver real en streaming: una query por tabla Silver declarada en config/data_sources.yml (clave `silver`) que lee lo que L2
// deja en Bronze (l2/<fuente>/<fecha>/part-*.parquet), le aplica el SQL de src/main/resources/silver/<tabla>.sql y escribe la
// tabla en Silver. Calcada de ParquetConverter (L2): una query por unidad, esquema fijado al arrancar, fuentes sin ficheros
// pendientes y reintentadas, marcador por micro-batch en el checkpoint y trazabilidad por batch.
//
// Modos (SilverTabla):
//   snapshot     cada descarga es una foto completa. El micro-batch solo DISPARA la reconstrucción: la entrada es la última
//                partición l2/<fuente>/<fecha>/ entera y el resultado SUSTITUYE la tabla (idempotente).
//   incremental  la entrada es el contenido del micro-batch (con _fecha_ingesta) y el resultado se AÑADE a la tabla.
//
// Quality gates (config/quality_gates.yml, silver_<tabla>) ANTES de escribir:
//   - un gate bloqueante fallido (o que no se puede evaluar) NO se publica: Silver se queda como estaba, el resultado va a
//     silver/_cuarentena/<tabla>/<fecha>-<batchId>/, la trazabilidad registra una fila `error` y el stream sigue vivo;
//   - un error del propio SQL o del esquema (código roto, no datos malos) se relanza: la query muere y el proceso termina.
//
// Cada query corre en su propia sesión clonada de Spark (batch.sparkSession): las vistas temporales `entrada` y
// `silver_<tabla>` de una tabla no se pisan con las de otra.
object SilverBuilder extends Logging {

  // Columnas auxiliares de la entrada de un SQL Silver (no analíticas).
  val FuenteCol = "_source_file"       // fichero Parquet de L2 del que viene la fila
  val IngestaCol = "_fecha_ingesta"    // fecha del directorio l2/<fuente>/<fecha>/ (la de la subida, no la del dato)

  private val Proceso = "silver_builder"
  private val MarkerDir = "raillytics-batches"
  private val CuarentenaDir = "_cuarentena"
  private val FechaDir = "[0-9]{4}-[0-9]{2}-[0-9]{2}"

  def l2Root(settings: SilverSettings, source: DataSource): String = s"${settings.bronzeRoot}/l2/${source.id}"

  def checkpointDir(settings: SilverSettings, tabla: SilverTabla): String = s"${settings.checkpointRoot}/silver/${tabla.tabla}"

  // Marcador de «batch ya procesado»: dentro del checkpoint de la query (borrar el checkpoint = reprocesar todo, también esto).
  def batchMarker(settings: SilverSettings, tabla: SilverTabla, batchId: Long): Path =
    new Path(s"${checkpointDir(settings, tabla)}/$MarkerDir/$batchId")

  def tablasDe(sources: Seq[DataSource]): Seq[(DataSource, SilverTabla)] =
    sources.flatMap(s => s.silver.map(t => (s, t)))

  // ------------------------------------------------------------------ query

  // Una query por tabla, con su propio checkpoint. El esquema de L2 se infiere UNA vez al arrancar con el lector batch: si L2
  // aún no ha escrito nada de la fuente, esto lanza y la tabla queda pendiente (ver startAll).
  def startQuery(source: DataSource, tabla: SilverTabla, settings: SilverSettings, hadoopConf: Configuration)
                (implicit spark: SparkSession): StreamingQuery = {
    logger.debug(s"preparando query de la tabla Silver '${tabla.tabla}' (fuente '${source.id}', modo ${tabla.modo})")
    val entrada = s"${l2Root(settings, source)}/*"   // l2/<fuente>/<fecha>/: se detectan las fechas nuevas
    val esquema = spark.read.parquet(entrada).schema
    spark.readStream.schema(esquema).parquet(entrada)
      .withColumn(FuenteCol, input_file_name())
      .writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) => procesarBatch(source, tabla, batch, batchId, hadoopConf, settings) }
      .option("checkpointLocation", checkpointDir(settings, tabla))
      .trigger(Trigger.ProcessingTime(settings.triggerIntervalMs))
      .start()
  }

  // Si L2 aún no ha dejado nada de la fuente (su estado normal si 04_silver arranca antes que L2 o con una fuente recién
  // registrada), Spark no puede inferir el esquema y la query no puede arrancar.
  private def isWaitingForFiles(e: Throwable): Boolean = e match {
    case ae: AnalysisException => Set("UNABLE_TO_INFER_SCHEMA", "PATH_NOT_FOUND").contains(ae.getCondition)
    case _                     => false
  }

  // Cada tabla arranca de forma independiente: sin el Try, el fallo de una tumbaría main() entero. Devuelve las que siguen
  // sin ficheros en L2, para reintentarlas más tarde; las que fallan por otro motivo se descartan con un aviso.
  def startAll(pendientes: Seq[(DataSource, SilverTabla)], settings: SilverSettings, hadoopConf: Configuration)
              (implicit spark: SparkSession): Seq[(DataSource, SilverTabla)] =
    pendientes.filter { case (source, tabla) =>
      Try(startQuery(source, tabla, settings, hadoopConf)) match {
        case Success(_) =>
          logger.info(s"query iniciada para la tabla Silver '${tabla.tabla}' (fuente '${source.id}')")
          false
        case Failure(e) if isWaitingForFiles(e) =>
          logger.debug(s"la fuente '${source.id}' aún no tiene ficheros en L2 (tabla '${tabla.tabla}')")
          true
        case Failure(e) =>
          logger.warn(s"no se pudo iniciar la query de la tabla Silver '${tabla.tabla}': ${e.getMessage}", e)
          false
      }
    }

  // ------------------------------------------------------------------ micro-batch

  def procesarBatch(source: DataSource, tabla: SilverTabla, batch: DataFrame, batchId: Long, hadoopConf: Configuration,
                    settings: SilverSettings): Unit = {
    implicit val spark: SparkSession = batch.sparkSession
    val marker = batchMarker(settings, tabla, batchId)
    val markerFs = marker.getFileSystem(hadoopConf)
    if (markerFs.exists(marker)) {
      logger.info(s"batch $batchId de '${tabla.tabla}' ya procesado en una ejecución anterior: no se vuelve a escribir")
      return
    }
    if (batch.isEmpty) return   // nada nuevo en L2

    val nombre = tabla.tabla
    val vista = LakeViews.SilverPrefix + nombre
    val parametros = Map("fuente" -> source.id, "tabla" -> nombre, "modo" -> tabla.modo, "batch" -> batchId)
    Cargas.registrar(Proceso, "silver", settings.cargasDir, parametros) { ejecucion =>
      val gates = new QualityGates.Acumulador
      try {
        entradaDe(source, tabla, batch, hadoopConf, settings).createOrReplaceTempView("entrada")
        val resultado = spark.sql(SilverSql.cargar(nombre)).cache()
        try {
          val n = resultado.count()
          resultado.createOrReplaceTempView(vista)
          // Se relee el YAML en cada batch: un gate retocado se aplica sin reiniciar el proceso.
          val evaluados = QualityGates.evaluar(vista, QualityGates.load(settings.calidadConfig).getOrElse(vista, Nil))
          evaluados.foreach(gates += _)
          val bloqueantes = evaluados.filter(_.bloquea)
          val origen = Some(l2Root(settings, source))
          if (bloqueantes.isEmpty) {
            val destino = settings.lake.silverTable(nombre)
            ejecucion.tabla(nombre, origen = origen, destino = Some(destino)) { carga =>
              escribir(resultado, tabla, destino)
              carga.filas = Some(n)
            }
          } else {
            val destino = s"${settings.lake.silverRoot}/$CuarentenaDir/$nombre/${LocalDate.now()}-$batchId/"
            ejecucion.tabla(nombre, origen = origen, destino = Some(destino)) { carga =>
              resultado.coalesce(1).write.mode("overwrite").parquet(destino)
              carga.estado = Cargas.EstadoError
              carga.error = Some(s"cuarentena: ${QualityGates.describirFallos(bloqueantes)}")
              carga.filas = Some(n)
            }
            logger.warn(s"tabla Silver '$nombre': batch $batchId en cuarentena ($destino), Silver se queda como estaba. " +
              QualityGates.describirFallos(bloqueantes))
          }
          escribirMarker(markerFs, marker)
        } finally resultado.unpersist()
      } finally QualityGates.registrar(gates.resultados, ejecucion.runId, Proceso, "silver", settings.calidadDir)
    }
  }

  // snapshot: la última partición ENTERA (no solo lo que trae este batch); incremental: el contenido del batch.
  private def entradaDe(source: DataSource, tabla: SilverTabla, batch: DataFrame, hadoopConf: Configuration,
                        settings: SilverSettings)(implicit spark: SparkSession): DataFrame =
    if (tabla.esSnapshot) ultimaFoto(source, hadoopConf, settings)
    else batch.withColumn(IngestaCol,
      regexp_extract(col(FuenteCol), s"/l2/[^/]+/($FechaDir)/", 1).cast("date"))

  // Todos los ficheros de la última partición l2/<fuente>/<AAAA-MM-DD>/, con las columnas auxiliares de entrada.
  private def ultimaFoto(source: DataSource, hadoopConf: Configuration, settings: SilverSettings)
                        (implicit spark: SparkSession): DataFrame = {
    val raiz = new Path(l2Root(settings, source))
    val fs: FileSystem = raiz.getFileSystem(hadoopConf)
    val fechas = fs.listStatus(raiz).filter(_.isDirectory).map(_.getPath.getName).filter(_.matches(FechaDir)).sorted
    require(fechas.nonEmpty, s"no hay particiones l2/${source.id}/<fecha>/ de las que reconstruir la tabla")
    val fecha = fechas.last
    spark.read.parquet(s"${raiz.toString}/$fecha")
      .withColumn(FuenteCol, input_file_name())
      .withColumn(IngestaCol, lit(fecha).cast("date"))
  }

  // snapshot: sustituye la tabla (un único fichero, como Gold); incremental: añade.
  private def escribir(df: DataFrame, tabla: SilverTabla, destino: String): Unit =
    if (tabla.esSnapshot) df.coalesce(1).write.mode("overwrite").parquet(destino)
    else df.write.mode("append").parquet(destino)

  // El marcador va DESPUÉS de escribir (o de mandar a cuarentena): si el proceso muere antes, el batch se repite; en snapshot
  // eso es idempotente (se vuelve a sustituir la tabla con la misma foto).
  private def escribirMarker(fs: FileSystem, marker: Path): Unit = {
    fs.mkdirs(marker.getParent)
    val out = fs.create(marker, true)
    try out.write("ok\n".getBytes(UTF_8)) finally out.close()
  }
}
