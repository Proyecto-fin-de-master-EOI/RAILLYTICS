package raillytics.ingesta.l1

import org.apache.spark.sql.SparkSession
import raillytics.common.logging.Logging
import raillytics.common.spark.SparkSessionFactory

// Punto de entrada de L1: solo cablea entorno, SparkSession y query.
// La lógica vive en RawUploader.
object RawUploaderApp extends Logging {

  def main(args: Array[String]): Unit = {
    val settings = RawUploaderSettings.fromEnv()

    logger.info(
      s"arrancando raw-uploader (stagingRoot=${settings.stagingRoot}, l1DoneRoot=${settings.l1DoneRoot}, " +
        s"checkpointRoot=${settings.checkpointRoot}, bronzeRoot=${settings.bronzeRoot})"
    )

    implicit val spark: SparkSession = SparkSessionFactory.buildForFileStreaming("raw-uploader")
    val query = RawUploader.startQuery(settings, spark.sparkContext.hadoopConfiguration)

    logger.info(s"query 'raw-uploader' iniciada (id=${query.id})")
    query.awaitTermination()
    logger.info("query 'raw-uploader' finalizada")
  }
}
