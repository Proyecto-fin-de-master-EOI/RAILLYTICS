package raillytics.common.lake

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.testutil.{TestPaths, TestSpark}

import java.nio.file.Files

class LakeViewsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private implicit var spark: SparkSession = _

  override def beforeAll(): Unit = spark = TestSpark.session("LakeViewsSpec")

  override def afterAll(): Unit = spark.stop()

  "LakeViews.ruta" should "map view names to the layer prefixes and reject unknown ones" in {
    val lake = LakeSettings("s3a://b", "s3a://s", "s3a://g", "s3a://g/_trazabilidad")
    LakeViews.ruta(lake, "silver_viajeros_enriquecidos") shouldBe Some("s3a://s/viajeros_enriquecidos/")
    LakeViews.ruta(lake, "gold_dim_fecha") shouldBe Some("s3a://g/dim_fecha/")
    LakeViews.ruta(lake, "bronze_x") shouldBe None
  }

  "LakeViews.registrar" should "register the readable prefixes and report the rest" in {
    val tmp = Files.createTempDirectory("lake-views-spec")
    val lake = LakeSettings(TestPaths.fileUri(tmp.resolve("bronze")), TestPaths.fileUri(tmp.resolve("silver")),
                            TestPaths.fileUri(tmp.resolve("gold")), TestPaths.fileUri(tmp.resolve("traza")))
    spark.createDataFrame(Seq(("a", 1), ("b", 2))).toDF("k", "v").write.parquet(lake.silverTable("tabla"))

    val fallidas = LakeViews.registrar(lake, Seq("silver_tabla", "gold_no_existe", "otra"))

    fallidas.keySet shouldBe Set("gold_no_existe", "otra")
    spark.sql("SELECT sum(v) FROM silver_tabla").collect().head.getLong(0) shouldBe 3L
  }
}
