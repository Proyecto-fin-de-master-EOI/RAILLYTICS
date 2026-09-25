package raillytics.ingesta.l1

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import raillytics.common.fs.HadoopFs
import raillytics.common.lake.BronzePaths
import raillytics.common.logging.Logging

import java.net.URI
import java.nio.file.Paths
import java.time.LocalDate

// L1: copia cada fichero del staging local tal cual a MinIO (l1-raw) y lo
// mueve a l1DoneRoot, de donde lo recoge L2.
object RawUploader extends Logging {

  def sourceIdFromLocalPath(uriString: String): String =
    Paths.get(new URI(uriString)).getParent.getFileName.toString

  def fileNameFromLocalPath(uriString: String): String =
    Paths.get(new URI(uriString)).getFileName.toString

  def processBatch(batch: DataFrame, hadoopConf: Configuration, bronzeRoot: String, l1DoneRoot: String): Unit = {
    val localFs = HadoopFs.local(hadoopConf)
    val destFs = HadoopFs.forRoot(bronzeRoot, hadoopConf)

    val paths = batch.select("path").collect().map(_.getString(0))
    logger.info(s"micro-batch de ${paths.length} fichero(s) recibido")

    paths.foreach { localUri =>
      val source = sourceIdFromLocalPath(localUri)
      val fileName = fileNameFromLocalPath(localUri)

      val srcPath = new Path(localUri)
      val destPath = new Path(BronzePaths.l1(bronzeRoot, source, LocalDate.now(), fileName))
      logger.debug(s"copiando '$srcPath' -> '$destPath' (fuente=$source)")
      FileUtil.copy(localFs, srcPath, destFs, destPath, false, hadoopConf)

      // l1DoneRoot es HERMANO del staging (no subdirectorio) — un subdirectorio
      // anidado sería recogido de nuevo por el glob de esta misma query.
      HadoopFs.moveInto(localFs, srcPath, new Path(s"$l1DoneRoot/$source"), fileName)
    }
  }

  def startQuery(settings: RawUploaderSettings, hadoopConf: Configuration)
                (implicit spark: SparkSession): StreamingQuery =
    spark.readStream
      .format("binaryFile")
      .load(s"${settings.stagingRoot}/*/*")   // data/bronze/<source>/<file> (plano, sin recursión)
      .writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        processBatch(batch, hadoopConf, settings.bronzeRoot, settings.l1DoneRoot)
      }
      .option("checkpointLocation", s"${settings.checkpointRoot}/l1-raw")
      .start()
}
