package raillytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, sum}

/** Ejemplo mínimo de uso de Spark SQL / DataFrames sobre datos de viajeros. */
object DataFrameExample {

  case class Viaje(estacion: String, linea: String, viajeros: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("raillytics-dataframe-example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val viajes = Seq(
      Viaje("Atocha", "C1", 12_450L),
      Viaje("Atocha", "C3", 8_200L),
      Viaje("Chamartín", "C1", 9_870L),
      Viaje("Chamartín", "C4", 6_530L),
      Viaje("Sol", "C3", 15_100L)
    ).toDF()

    val viajerosPorEstacion: DataFrame = viajes
      .groupBy(col("estacion"))
      .agg(sum("viajeros").as("total_viajeros"))
      .orderBy(col("total_viajeros").desc)

    viajerosPorEstacion.show()

    spark.stop()
  }
}
