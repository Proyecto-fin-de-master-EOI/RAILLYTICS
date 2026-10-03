package raillytics.silver

import org.apache.spark.sql.SparkSession
import raillytics.common.calidad.QualityGates
import raillytics.common.config.AppConfig
import raillytics.common.lake.LakeViews
import raillytics.common.logging.Logging
import raillytics.common.spark.SparkSessionFactory
import raillytics.ingesta.config.DataSourceConfig

// Punto de entrada de `make 04_silver`: solo cablea configuración, SparkSession y queries; la lógica vive en SilverBuilder.
// Se queda en primer plano (como 01_raw-uploader y 02_parquet-converter): lánzalo en una terminal aparte.
object SilverBuilderApp extends Logging {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val settings = SilverSettings.from(config)

    logger.info(s"arrancando silver-builder (configPath=${settings.configPath}, bronzeRoot=${settings.bronzeRoot}, " +
      s"silverRoot=${settings.lake.silverRoot}, checkpointRoot=${settings.checkpointRoot}, trigger=${settings.triggerIntervalMs} ms)")

    // Falla pronto lo que es de configuración (no de datos): sin tablas declaradas, SQL ausente o YAML de gates roto.
    val sources = DataSourceConfig.load(settings.configPath)
    var pendientes = SilverBuilder.tablasDe(sources)
    require(pendientes.nonEmpty, s"ninguna fuente de ${settings.configPath} declara tablas Silver (clave 'silver')")
    pendientes.foreach { case (_, tabla) => SilverSql.cargar(tabla.tabla) }
    val gates = QualityGates.load(settings.calidadConfig)
    pendientes.map(_._2.tabla).filterNot(t => gates.contains(LakeViews.SilverPrefix + t)).foreach { t =>
      logger.warn(s"la tabla Silver '$t' no tiene gates en ${settings.calidadConfig}: se publicará sin control de calidad")
    }

    implicit val spark: SparkSession = SparkSessionFactory.buildForFileStreaming("silver-builder", config)
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    pendientes = SilverBuilder.startAll(pendientes, settings, hadoopConf)

    // Si todas las tablas fallan al arrancar, awaitAnyTermination() se quedaría bloqueado sin ninguna query viva: este aviso
    // hace visible ese estado en vez de que el proceso parezca «vivo» sin hacer nada.
    if (spark.streams.active.isEmpty && pendientes.isEmpty) {
      logger.warn("ninguna query se pudo iniciar; el proceso quedará bloqueado sin trabajo que hacer")
    } else {
      logger.info(s"${spark.streams.active.length} query(s) activa(s), esperando a que termine alguna")
    }
    if (pendientes.nonEmpty) {
      logger.info(
        s"tablas Silver cuya fuente aún no tiene ficheros en L2 (${pendientes.map(_._2.tabla).mkString(", ")}): " +
          s"se reintentará arrancarlas cada ${settings.pendingRetryMs / 1000} s"
      )
    }

    // Si una query muere (un SQL roto, un esquema inesperado), todo el proceso termina en vez de quedarse a medias con unas
    // tablas vivas y otras muertas en silencio: awaitAnyTermination relanza su excepción, o devuelve true si paró sin error.
    var terminada = false
    while (pendientes.nonEmpty && !terminada) {
      terminada = spark.streams.awaitAnyTermination(settings.pendingRetryMs)
      if (!terminada) pendientes = SilverBuilder.startAll(pendientes, settings, hadoopConf)
    }
    if (!terminada) spark.streams.awaitAnyTermination()
  }
}
