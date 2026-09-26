package raillytics.ingesta

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.common.config.AppConfig
import raillytics.ingesta.l1.RawUploaderSettings
import raillytics.ingesta.l2.ParquetConverterSettings

class IngestaSettingsSpec extends AnyFlatSpec with Matchers {

  "RawUploaderSettings.from" should "use the application.conf defaults when no variable is set" in {
    RawUploaderSettings.from(AppConfig.defaults()) shouldBe RawUploaderSettings(
      stagingRoot = "data/bronze",
      l1DoneRoot = "data/bronze_l1_done",
      checkpointRoot = "data/checkpoints",
      bronzeRoot = "s3a://raillytics-bronze",
      cargasDir = "s3a://raillytics-gold/_trazabilidad/cargas/",
      calidadDir = "s3a://raillytics-gold/_trazabilidad/calidad/"
    )
  }

  "ParquetConverterSettings.from" should "use the application.conf defaults when no variable is set" in {
    ParquetConverterSettings.from(AppConfig.defaults()) shouldBe ParquetConverterSettings(
      configPath = "config/data_sources.yml",
      l1DoneRoot = "data/bronze_l1_done",
      processedRoot = "data/bronze_processed",
      rejectedRoot = "data/bronze_rejected",
      checkpointRoot = "data/checkpoints",
      bronzeRoot = "s3a://raillytics-bronze",
      cargasDir = "s3a://raillytics-gold/_trazabilidad/cargas/",
      calidadDir = "s3a://raillytics-gold/_trazabilidad/calidad/",
      pendingRetryMs = 30000L
    )
  }

  "L1 and L2 settings" should "resolve the shared roots from the same variables" in {
    val config = AppConfig.defaults(
      "L1_DONE_ROOT = /x/done\nCHECKPOINT_ROOT = /x/chk\nMINIO_BUCKET_BRONZE = otro-bucket"
    )
    val l1 = RawUploaderSettings.from(config)
    val l2 = ParquetConverterSettings.from(config)

    l1.l1DoneRoot shouldBe "/x/done"
    l2.l1DoneRoot shouldBe l1.l1DoneRoot
    l2.checkpointRoot shouldBe l1.checkpointRoot
    l2.bronzeRoot shouldBe "s3a://otro-bucket"
    l2.cargasDir shouldBe l1.cargasDir
  }

  it should "send the load records to the Gold bucket, where the Python side also writes them" in {
    RawUploaderSettings.from(AppConfig.defaults("MINIO_BUCKET_GOLD = otro-gold")).cargasDir shouldBe
      "s3a://otro-gold/_trazabilidad/cargas/"
    ParquetConverterSettings.from(AppConfig.defaults("TRAZABILIDAD_ROOT = \"file:///tmp/traza\"")).cargasDir shouldBe
      "file:///tmp/traza/cargas/"
  }
}
