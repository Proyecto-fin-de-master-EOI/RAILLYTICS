package raillytics.ingesta.l2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.streaming.StreamingQuery
import raillytics.common.fs.HadoopFs
import raillytics.common.lake.BronzePaths
import raillytics.common.logging.Logging
import raillytics.common.trazabilidad.Cargas
import raillytics.ingesta.config.DataSource
import raillytics.ingesta.formats.SourceFormat

import java.time.LocalDate
import scala.util.{Failure, Success, Try}

// L2: una query por fuente que lee lo que L1 dejó en l1DoneRoot, lo escribe
// como Parquet en MinIO (l2) y mueve los ficheros a processedRoot.
object ParquetConverter extends Logging {

  // Columna de bookkeeping (no analítica): readStream sobre csv/json no expone
  // "path" como sí hace binaryFile, así que hace falta input_file_name() para
  // saber qué ficheros componen cada micro-batch y poder moverlos después.
  val SourceFileCol = "_source_file"

  // Mueve los ficheros ya convertidos de l1DoneRoot a processedRoot/<fuente>: así
  // no vuelven a entrar en ningún glob y queda claro qué completó L1 y L2.
  def moveProcessedFiles(filePaths: Seq[String], hadoopConf: Configuration, source: String, processedRoot: String): Unit = {
    val localFs = HadoopFs.local(hadoopConf)
    val destDir = new Path(s"$processedRoot/$source")

    filePaths.foreach { localUri =>
      val srcPath = new Path(localUri)
      logger.debug(s"moviendo '$srcPath' -> '$destDir' (fuente=$source)")
      HadoopFs.moveInto(localFs, srcPath, destDir)
    }
  }

  // Procesa un micro-batch de una fuente: escribe su Parquet en l2/<fuente>/<fecha>/
  // y mueve los ficheros de origen a processedRoot. Si la escritura falla no se
  // mueve nada y el checkpoint no avanza, así que el siguiente arranque los reintenta.
  def processBatch(source: DataSource, batch: DataFrame, hadoopConf: Configuration, bronzeRoot: String,
                   processedRoot: String, cargasDir: String): Unit = {
    implicit val spark: SparkSession = batch.sparkSession
    // El batch se evalúa varias veces (ficheros, escritura, recuento): cacheado
    // se leen y parsean los ficheros una sola vez.
    batch.persist()
    try {
      val filesInBatch = batch.select(SourceFileCol).distinct().collect().map(_.getString(0)).toSeq
      logger.info(s"micro-batch de la fuente '${source.id}': ${filesInBatch.size} fichero(s)")
      // Micro-batch vacío: nada que escribir ni que registrar (el return no se
      // salta el finally, el unpersist se hace igual).
      if (filesInBatch.isEmpty) return

      val destPath = BronzePaths.l2(bronzeRoot, source.id, LocalDate.now())
      val parametros = Map("fuente" -> source.id, "formato" -> source.format, "ficheros" -> filesInBatch.size)
      // Trazabilidad: una fila por micro-batch y fuente (filas = registros escritos en Parquet).
      Cargas.registrar("bronze_l2_parquet_converter", "bronze", cargasDir, parametros) { ejecucion =>
        val origen = filesInBatch.headOption.map(file => new Path(file).getParent.toString)
        ejecucion.tabla(source.id, origen = origen, destino = Some(destPath)) { carga =>
          batch.drop(SourceFileCol).write.mode("append").parquet(destPath)
          logger.debug(s"batch de '${source.id}' escrito en '$destPath'")

          moveProcessedFiles(filesInBatch, hadoopConf, source.id, processedRoot)
          carga.filas = Some(batch.count())
        }
      }
    } finally batch.unpersist()
  }

  // Una query por fuente, con su propio checkpoint: cada una avanza (y falla) de
  // forma independiente, y el esquema se infiere por fuente, no mezclando formatos.
  def startQuery(source: DataSource, settings: ParquetConverterSettings, hadoopConf: Configuration)
                (implicit spark: SparkSession): StreamingQuery = {
    logger.debug(s"preparando query de la fuente '${source.id}' (formato=${source.format})")
    SourceFormat.streamReader(source.format)
      .load(s"${settings.l1DoneRoot}/${source.id}/*")  // plano, sin recursión (hermano de data/bronze/)
      .withColumn(SourceFileCol, input_file_name())
      .writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        processBatch(source, batch, hadoopConf, settings.bronzeRoot, settings.processedRoot, settings.cargasDir)
      }
      .option("checkpointLocation", s"${settings.checkpointRoot}/l2/${source.id}")
      .start()
  }

  // Si el directorio L1 de una fuente está vacío (su estado normal en reposo,
  // si L2 arranca antes que L1, o una fuente recién registrada), Spark no
  // puede inferir el esquema y readStream falla al arrancar la query.
  private def isWaitingForFiles(e: Throwable): Boolean = e match {
    case ae: AnalysisException => ae.getCondition == "UNABLE_TO_INFER_SCHEMA"
    case _                     => false
  }

  // Cada fuente arranca su query de forma independiente: sin el Try, el fallo
  // de una tumbaría main() entero y ninguna otra fuente (aunque esté sana)
  // llegaría a arrancar. Devuelve las fuentes que siguen sin ficheros en L1,
  // para reintentarlas más tarde; las que fallan por otro motivo se descartan.
  def startAll(sources: Seq[DataSource], settings: ParquetConverterSettings, hadoopConf: Configuration)
              (implicit spark: SparkSession): Seq[DataSource] =
    sources.filter { source =>
      Try(startQuery(source, settings, hadoopConf)) match {
        case Success(_) =>
          logger.info(s"query iniciada para la fuente '${source.id}'")
          false
        case Failure(e) if isWaitingForFiles(e) =>
          logger.debug(s"la fuente '${source.id}' aún no tiene ficheros en L1")
          true
        case Failure(e) =>
          logger.warn(s"no se pudo iniciar la query para '${source.id}': ${e.getMessage}", e)
          false
      }
    }
}
