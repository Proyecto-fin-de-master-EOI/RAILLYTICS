package raillytics.ingesta.l1

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.testutil.TestPaths

import java.nio.file.{Files, Path}

class RawUploaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("RawUploaderSpec").master("local[1]").getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  "RawUploader.processBatch" should "copy the file as-is to bronzeRoot and move it to l1DoneRoot" in {
    val tmpDir: Path = Files.createTempDirectory("raw-uploader-spec")
    val stagingDir = tmpDir.resolve("staging/crtm")
    Files.createDirectories(stagingDir)
    Files.writeString(stagingDir.resolve("sample.csv"), "estacion,viajeros\nAtocha,100\n")

    val bronzeRoot = TestPaths.fileUri(tmpDir.resolve("bronze"))
    val l1DoneRoot = tmpDir.resolve("l1_done").toString

    val batch = spark.read.format("binaryFile").load(s"${tmpDir.resolve("staging")}/*/*")
    RawUploader.processBatch(batch, spark.sparkContext.hadoopConfiguration, bronzeRoot, l1DoneRoot)

    val today = java.time.LocalDate.now()
    val expectedRawFile = tmpDir.resolve(s"bronze/l1-raw/crtm/$today/sample.csv")
    Files.exists(expectedRawFile) shouldBe true
    Files.readString(expectedRawFile) shouldBe "estacion,viajeros\nAtocha,100\n"

    Files.exists(stagingDir.resolve("sample.csv")) shouldBe false
    Files.exists(tmpDir.resolve("l1_done/crtm/sample.csv")) shouldBe true
  }
}
