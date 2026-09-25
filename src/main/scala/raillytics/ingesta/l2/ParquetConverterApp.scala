package raillytics.ingesta.l2

import org.apache.spark.sql.SparkSession
import raillytics.common.logging.Logging
import raillytics.common.spark.SparkSessionFactory
import raillytics.ingesta.config.DataSourceConfig

// Punto de entrada de L2: solo cablea entorno, SparkSession y queries.
// La lógica vive en ParquetConverter.
object ParquetConverterApp extends Logging {

  def main(args: Array[String]): Unit = {
    val settings = ParquetConverterSettings.fromEnv()

    logger.info(
      s"arrancando parquet-converter (configPath=${settings.configPath}, l1DoneRoot=${settings.l1DoneRoot}, " +
        s"processedRoot=${settings.processedRoot}, checkpointRoot=${settings.checkpointRoot}, " +
        s"bronzeRoot=${settings.bronzeRoot})"
    )

    implicit val spark: SparkSession = SparkSessionFactory.buildForFileStreaming("parquet-converter")
    val sources = DataSourceConfig.load(settings.configPath)
    val startedQueries = ParquetConverter.startAll(sources, settings, spark.sparkContext.hadoopConfiguration)

    // Si todas las fuentes fallan al arrancar, awaitAnyTermination() se queda
    // bloqueado indefinidamente sin ninguna query viva -- este aviso hace visible
    // ese estado en vez de que el proceso parezca "vivo" sin hacer nada.
    if (startedQueries == 0) {
      logger.warn("ninguna query se pudo iniciar; el proceso quedará bloqueado sin trabajo que hacer")
    } else {
      logger.info(s"$startedQueries query(s) activa(s), esperando a que termine alguna")
    }

    // Si una query muere (p.ej. fichero mal formado), todo el proceso termina en
    // vez de quedarse a medias con unas fuentes vivas y otras muertas en silencio.
    spark.streams.awaitAnyTermination()
  }
}
