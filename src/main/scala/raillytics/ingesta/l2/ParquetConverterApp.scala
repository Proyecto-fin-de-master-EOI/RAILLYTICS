package raillytics.ingesta.l2

import org.apache.spark.sql.SparkSession
import raillytics.common.config.AppConfig
import raillytics.common.logging.Logging
import raillytics.common.spark.SparkSessionFactory
import raillytics.ingesta.config.DataSourceConfig

// Punto de entrada de L2: solo cablea configuración, SparkSession y queries.
// La lógica vive en ParquetConverter.
object ParquetConverterApp extends Logging {

  def main(args: Array[String]): Unit = {
    // Configuración (application.conf + entorno + .env): la misma para Spark y para la app.
    val config = AppConfig.load()
    val settings = ParquetConverterSettings.from(config)

    logger.info(
      s"arrancando parquet-converter (configPath=${settings.configPath}, l1DoneRoot=${settings.l1DoneRoot}, " +
        s"processedRoot=${settings.processedRoot}, checkpointRoot=${settings.checkpointRoot}, " +
        s"bronzeRoot=${settings.bronzeRoot})"
    )

    implicit val spark: SparkSession = SparkSessionFactory.buildForFileStreaming("parquet-converter", config)
    // Una query por cada fuente del YAML; las que aún no tienen ficheros en L1 quedan pendientes.
    val sources = DataSourceConfig.load(settings.configPath)
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    var pending = ParquetConverter.startAll(sources, settings, hadoopConf)

    // Si todas las fuentes fallan al arrancar, awaitAnyTermination() se queda
    // bloqueado indefinidamente sin ninguna query viva -- este aviso hace visible
    // ese estado en vez de que el proceso parezca "vivo" sin hacer nada.
    if (spark.streams.active.isEmpty && pending.isEmpty) {
      logger.warn("ninguna query se pudo iniciar; el proceso quedará bloqueado sin trabajo que hacer")
    } else {
      logger.info(s"${spark.streams.active.length} query(s) activa(s), esperando a que termine alguna")
    }
    if (pending.nonEmpty) {
      logger.info(
        s"fuentes aún sin ficheros en L1 (${pending.map(_.id).mkString(", ")}): " +
          s"se reintentará arrancarlas cada ${settings.pendingRetryMs / 1000} s"
      )
    }

    // Si una query muere (p.ej. fichero mal formado), todo el proceso termina en
    // vez de quedarse a medias con unas fuentes vivas y otras muertas en silencio:
    // awaitAnyTermination relanza su excepción, o devuelve true si paró sin error.
    var anyTerminated = false
    while (pending.nonEmpty && !anyTerminated) {
      anyTerminated = spark.streams.awaitAnyTermination(settings.pendingRetryMs)
      if (!anyTerminated) pending = ParquetConverter.startAll(pending, settings, hadoopConf)
    }
    if (!anyTerminated) spark.streams.awaitAnyTermination()
  }
}
