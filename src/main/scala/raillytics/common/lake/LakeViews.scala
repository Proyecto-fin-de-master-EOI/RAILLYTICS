package raillytics.common.lake

import org.apache.spark.sql.SparkSession
import raillytics.common.logging.Logging

import scala.util.{Failure, Success, Try}

// Registra tablas del lake como vistas temporales de la sesión con el nombre
// <capa>_<tabla> (silver_viajeros_enriquecidos, gold_dim_fecha...), que es como
// las referencian el SQL de Gold (src/main/resources/gold/*.sql) y los quality
// gates (config/quality_gates.yml).
object LakeViews extends Logging {

  val SilverPrefix = "silver_"
  val GoldPrefix = "gold_"

  // Ruta en el lake de una vista por su nombre, o None si el prefijo no es de una capa.
  def ruta(lake: LakeSettings, vista: String): Option[String] =
    if (vista.startsWith(SilverPrefix)) Some(lake.silverTable(vista.stripPrefix(SilverPrefix)))
    else if (vista.startsWith(GoldPrefix)) Some(lake.goldTable(vista.stripPrefix(GoldPrefix)))
    else None

  // Registra cada vista leyendo su prefijo Parquet entero. Devuelve las que no se
  // pudieron registrar (prefijo inexistente, sin ficheros...): quien llama decide
  // si es un error (Gold sin Silver) o solo un aviso (validar Gold sin tener Silver).
  def registrar(lake: LakeSettings, vistas: Seq[String])(implicit spark: SparkSession): Map[String, Throwable] =
    vistas.distinct.flatMap { vista =>
      ruta(lake, vista) match {
        case None =>
          Some(vista -> new IllegalArgumentException(s"la vista '$vista' no empieza por $SilverPrefix ni $GoldPrefix"))
        case Some(path) =>
          Try(spark.read.parquet(path).createOrReplaceTempView(vista)) match {
            case Success(_) =>
              logger.debug(s"vista '$vista' registrada sobre $path")
              None
            case Failure(e) =>
              logger.warn(s"no se pudo registrar la vista '$vista' sobre $path: ${e.getMessage}")
              Some(vista -> e)
          }
      }
    }.toMap
}
