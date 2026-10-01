package raillytics.calidad

import org.apache.spark.sql.SparkSession
import raillytics.common.calidad.QualityGates
import raillytics.common.config.AppConfig
import raillytics.common.lake.LakeViews
import raillytics.common.logging.Logging
import raillytics.common.spark.SparkSessionFactory
import raillytics.common.trazabilidad.Cargas

// Quality Gate independiente sobre el lake (batch): evalúa config/quality_gates.yml
// contra lo que hay en Silver y Gold, registra los resultados en
// <trazabilidad>/calidad/ y termina con error si falla algún gate bloqueante.
// No escribe datos: sirve como paso de control entre capas (después de Silver,
// después de Gold) o para auditar el lake en cualquier momento.
//
//   make quality-gates                 (todas las tablas del YAML)
//   sbt "runMain raillytics.calidad.QualityGatesApp silver"        (solo silver_*)
//   sbt "runMain raillytics.calidad.QualityGatesApp gold_dim_fecha" (una tabla)
object QualityGatesApp extends Logging {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val settings = QualityGatesSettings.from(config)
    logger.info(s"arrancando quality-gates (config=${settings.calidadConfig}, silverRoot=${settings.lake.silverRoot}, " +
      s"goldRoot=${settings.lake.goldRoot}, calidadDir=${settings.lake.calidadDir})")

    implicit val spark: SparkSession = SparkSessionFactory.build("quality-gates", config)
    try {
      val gates = QualityGates.load(settings.calidadConfig)
      val tablas = seleccionar(args.toSeq, gates.keys.toSeq)
      val capa = capaDe(tablas)
      require(tablas.nonEmpty, s"ningún gate coincide con '${args.mkString(" ")}' (tablas: ${gates.keys.mkString(", ")})")

      val resultados = Cargas.registrar("quality_gates", capa, settings.lake.cargasDir, Map("tablas" -> tablas)) { ejecucion =>
        // Se registran todas las vistas del YAML (también las referenciadas por
        // gates de otras tablas, p. ej. Silver para conciliar Gold); las que no
        // existen en el lake hacen fallar sus gates con resultado 'error'.
        LakeViews.registrar(settings.lake, gates.keys.toSeq ++ gates.values.flatten.flatMap(_.tabla))
        val res = QualityGates.evaluarTablas(gates, tablas)
        QualityGates.registrar(res, ejecucion.runId, "quality_gates", capa, settings.lake.calidadDir)
        println(QualityGates.resumen(res))
        QualityGates.exigir(res)   // falla el proceso (y la carga queda con estado error)
        res
      }
      println(s"${resultados.size} gate(s) evaluados sobre ${tablas.size} tabla(s): todos los bloqueantes OK")
    } finally spark.stop()
  }

  // Sin argumentos, todas las tablas; "silver" / "gold" filtran por capa; cualquier
  // otro argumento es el nombre exacto de una tabla del YAML.
  def seleccionar(args: Seq[String], tablas: Seq[String]): Seq[String] =
    if (args.isEmpty) tablas
    else tablas.filter(t => args.exists(a => a == t || (a == "silver" && t.startsWith(LakeViews.SilverPrefix)) ||
                                                        (a == "gold" && t.startsWith(LakeViews.GoldPrefix))))

  private def capaDe(tablas: Seq[String]): String =
    if (tablas.forall(_.startsWith(LakeViews.SilverPrefix))) "silver"
    else if (tablas.forall(_.startsWith(LakeViews.GoldPrefix))) "gold"
    else "lake"
}
