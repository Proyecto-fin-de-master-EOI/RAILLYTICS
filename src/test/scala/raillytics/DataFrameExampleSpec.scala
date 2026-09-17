package raillytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataFrameExampleSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder()
    .appName("DataFrameExampleSpec")
    .master("local[1]")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
  }

  "DataFrameExample" should "agregar los viajeros totales por estación" in {
    import spark.implicits._

    val viajes = Seq(
      DataFrameExample.Viaje("Atocha", "C1", 100L),
      DataFrameExample.Viaje("Atocha", "C3", 50L),
      DataFrameExample.Viaje("Sol", "C3", 30L)
    ).toDF()

    val resultado = viajes
      .groupBy(col("estacion"))
      .agg(sum("viajeros").as("total_viajeros"))
      .collect()
      .map(row => row.getString(0) -> row.getLong(1))
      .toMap

    resultado("Atocha") shouldBe 150L
    resultado("Sol") shouldBe 30L
  }
}
