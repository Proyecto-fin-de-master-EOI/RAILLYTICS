package raillytics.gold

import org.apache.spark.sql.SparkSession
import raillytics.common.logging.Logging
import raillytics.common.spark.SparkSessionFactory

// Punto de entrada de la construcción de Gold con Spark (batch): solo cablea
// entorno y SparkSession. La lógica vive en GoldBuilder.
//   make 04_gold   (sbt "runMain raillytics.gold.GoldBuilderApp")
object GoldBuilderApp extends Logging {

  def main(args: Array[String]): Unit = {
    val settings = GoldBuilderSettings.fromEnv()
    logger.info(
      s"arrancando gold-builder (silverRoot=${settings.silverRoot}, goldRoot=${settings.goldRoot}, " +
        s"cargasDir=${settings.cargasDir})"
    )

    implicit val spark: SparkSession = SparkSessionFactory.build("gold-builder")
    try {
      val counts = GoldBuilder.build(settings)
      // Resumen por consola para quien lanza el job (el detalle va al log y a la trazabilidad).
      println(s"Silver: ${settings.silverRoot}  ->  Gold: ${settings.goldRoot}")
      GoldBuilder.GoldTables.foreach(table => println(f"  $table%-18s${counts(table)}%,10d filas"))
    } finally spark.stop()  // también si build falla: un job batch no debe dejar el contexto abierto
  }
}
