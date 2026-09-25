package raillytics.ingesta.l1

import org.apache.spark.sql.SparkSession
import raillytics.common.config.AppConfig
import raillytics.common.logging.Logging
import raillytics.common.spark.SparkSessionFactory

// Punto de entrada de L1: solo cablea configuración, SparkSession y query.
// La lógica vive en RawUploader.
object RawUploaderApp extends Logging {

  def main(args: Array[String]): Unit = {
    // Configuración (application.conf + entorno + .env): la misma para Spark y para la app.
    val config = AppConfig.load()
    val settings = RawUploaderSettings.from(config)

    logger.info(
      s"arrancando raw-uploader (stagingRoot=${settings.stagingRoot}, l1DoneRoot=${settings.l1DoneRoot}, " +
        s"checkpointRoot=${settings.checkpointRoot}, bronzeRoot=${settings.bronzeRoot})"
    )

    implicit val spark: SparkSession = SparkSessionFactory.buildForFileStreaming("raw-uploader", config)
    val query = RawUploader.startQuery(settings, spark.sparkContext.hadoopConfiguration)

    logger.info(s"query 'raw-uploader' iniciada (id=${query.id})")
    // Se queda en primer plano hasta que se pare (Ctrl+C) o la query muera por
    // error: en ese caso awaitTermination relanza la excepción y el proceso acaba.
    query.awaitTermination()
    logger.info("query 'raw-uploader' finalizada")
  }
}
