package raillytics.ingesta.l2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.streaming.StreamingQuery
import raillytics.common.fs.HadoopFs
import raillytics.common.lake.BronzePaths
import raillytics.common.logging.Logging
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

  def moveProcessedFiles(filePaths: Seq[String], hadoopConf: Configuration, source: String, processedRoot: String): Unit = {
    val localFs = HadoopFs.local(hadoopConf)
    val destDir = new Path(s"$processedRoot/$source")

    filePaths.foreach { localUri =>
      val srcPath = new Path(localUri)
      logger.debug(s"moviendo '$srcPath' -> '$destDir' (fuente=$source)")
      HadoopFs.moveInto(localFs, srcPath, destDir)
    }
  }

  def processBatch(source: DataSource, batch: DataFrame, hadoopConf: Configuration, bronzeRoot: String, processedRoot: String): Unit = {
    val filesInBatch = batch.select(SourceFileCol).distinct().collect().map(_.getString(0)).toSeq
    logger.info(s"micro-batch de la fuente '${source.id}': ${filesInBatch.size} fichero(s)")

    val destPath = BronzePaths.l2(bronzeRoot, source.id, LocalDate.now())
    batch.drop(SourceFileCol).write.mode("append").parquet(destPath)
    logger.debug(s"batch de '${source.id}' escrito en '$destPath'")

    moveProcessedFiles(filesInBatch, hadoopConf, source.id, processedRoot)
  }

  def startQuery(source: DataSource, settings: ParquetConverterSettings, hadoopConf: Configuration)
                (implicit spark: SparkSession): StreamingQuery = {
    logger.debug(s"preparando query de la fuente '${source.id}' (formato=${source.format})")
    SourceFormat.streamReader(source.format)
      .load(s"${settings.l1DoneRoot}/${source.id}/*")  // plano, sin recursión (hermano de data/bronze/)
      .withColumn(SourceFileCol, input_file_name())
      .writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        processBatch(source, batch, hadoopConf, settings.bronzeRoot, settings.processedRoot)
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
