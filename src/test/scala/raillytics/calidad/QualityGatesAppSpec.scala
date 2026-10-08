package raillytics.calidad

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QualityGatesAppSpec extends AnyFlatSpec with Matchers {

  private val tablas = Seq("silver_viajeros", "silver_cnmc", "gold_fact_mercado")
  private val opcionales = Set("silver_cnmc", "gold_fact_mercado")

  "QualityGatesApp.omitidas" should "skip optional tables whose view could not be registered" in {
    QualityGatesApp.omitidas(tablas, opcionales, pedidas = Set.empty, sinDatos = Set("silver_cnmc", "gold_fact_mercado")) shouldBe
      Seq("silver_cnmc", "gold_fact_mercado")
  }

  it should "never skip a required table, even when it has no data (its gates must fail)" in {
    QualityGatesApp.omitidas(tablas, opcionales, pedidas = Set.empty, sinDatos = Set("silver_viajeros")) shouldBe empty
  }

  it should "not skip an optional table that does have data" in {
    QualityGatesApp.omitidas(tablas, opcionales, pedidas = Set.empty, sinDatos = Set("gold_fact_mercado")) shouldBe Seq("gold_fact_mercado")
  }

  it should "treat an optional table asked for by name as required" in {
    QualityGatesApp.omitidas(tablas, opcionales, pedidas = Set("silver_cnmc"), sinDatos = Set("silver_cnmc")) shouldBe empty
  }
}
