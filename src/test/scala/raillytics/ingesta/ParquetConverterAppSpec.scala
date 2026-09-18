package raillytics.ingesta

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class ParquetConverterAppSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("ParquetConverterAppSpec").master("local[1]").getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  "ParquetConverterApp.processBatch" should "convert csv to parquet and move the file to processedRoot" in {
    val tmpDir: Path = Files.createTempDirectory("parquet-converter-spec")
    val l1DoneDir = tmpDir.resolve("l1_done/crtm")
    Files.createDirectories(l1DoneDir)
    Files.writeString(l1DoneDir.resolve("sample.csv"), "estacion,viajeros\nAtocha,100\nSol,50\n")

    val bronzeRoot = s"file://${tmpDir.resolve("bronze")}"
    val processedRoot = tmpDir.resolve("processed").toString
    val source = DataSource("crtm", "CRTM test", "https://example.invalid", "csv")

    val batch = spark.read.format("csv").option("header", "true")
      .load(s"${tmpDir.resolve("l1_done/crtm")}/*")
      .withColumn(ParquetConverterApp.SourceFileCol, input_file_name())

    ParquetConverterApp.processBatch(source, batch, spark.sparkContext.hadoopConfiguration, bronzeRoot, processedRoot)

    val today = java.time.LocalDate.now()
    val parquetDir = tmpDir.resolve(s"bronze/l2/crtm/$today")
    Files.exists(parquetDir) shouldBe true
    Files.list(parquetDir).iterator().asScala.exists(_.toString.endsWith(".parquet")) shouldBe true

    val result = spark.read.parquet(BronzePaths.l2(bronzeRoot, "crtm", today))
    result.count() shouldBe 2
    result.columns should contain theSameElementsAs Seq("estacion", "viajeros")

    Files.exists(l1DoneDir.resolve("sample.csv")) shouldBe false
    Files.exists(tmpDir.resolve("processed/crtm/sample.csv")) shouldBe true
  }
}
