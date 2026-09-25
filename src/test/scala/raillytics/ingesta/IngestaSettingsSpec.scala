package raillytics.ingesta

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.ingesta.l1.RawUploaderSettings
import raillytics.ingesta.l2.ParquetConverterSettings

class IngestaSettingsSpec extends AnyFlatSpec with Matchers {

  "RawUploaderSettings.fromEnv" should "use the defaults when no variable is set" in {
    RawUploaderSettings.fromEnv(Map.empty) shouldBe RawUploaderSettings(
      stagingRoot = "data/bronze",
      l1DoneRoot = "data/bronze_l1_done",
      checkpointRoot = "data/checkpoints",
      bronzeRoot = "s3a://raillytics-bronze"
    )
  }

  "ParquetConverterSettings.fromEnv" should "use the defaults when no variable is set" in {
    ParquetConverterSettings.fromEnv(Map.empty) shouldBe ParquetConverterSettings(
      configPath = "config/data_sources.yml",
      l1DoneRoot = "data/bronze_l1_done",
      processedRoot = "data/bronze_processed",
      checkpointRoot = "data/checkpoints",
      bronzeRoot = "s3a://raillytics-bronze"
    )
  }

  "L1 and L2 settings" should "resolve the shared roots from the same variables" in {
    val env = Map(
      "L1_DONE_ROOT" -> "/x/done",
      "CHECKPOINT_ROOT" -> "/x/chk",
      "MINIO_BUCKET_BRONZE" -> "otro-bucket"
    )
    val l1 = RawUploaderSettings.fromEnv(env)
    val l2 = ParquetConverterSettings.fromEnv(env)

    l1.l1DoneRoot shouldBe "/x/done"
    l2.l1DoneRoot shouldBe l1.l1DoneRoot
    l2.checkpointRoot shouldBe l1.checkpointRoot
    l2.bronzeRoot shouldBe "s3a://otro-bucket"
  }
}
