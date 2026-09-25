package raillytics.common.lake

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.common.config.AppConfig

class LakeSettingsSpec extends AnyFlatSpec with Matchers {

  "LakeSettings" should "default to the MinIO buckets with the s3a scheme" in {
    val lake = LakeSettings.from(AppConfig.defaults())
    lake.bronzeRoot shouldBe "s3a://raillytics-bronze"
    lake.silverRoot shouldBe "s3a://raillytics-silver"
    lake.goldRoot shouldBe "s3a://raillytics-gold"
    lake.trazabilidadRoot shouldBe "s3a://raillytics-gold/_trazabilidad"
    lake.cargasDir shouldBe "s3a://raillytics-gold/_trazabilidad/cargas/"
    lake.goldTable("dim_fecha") shouldBe "s3a://raillytics-gold/dim_fecha/"
    lake.silverTable("viajeros_enriquecidos") shouldBe "s3a://raillytics-silver/viajeros_enriquecidos/"
  }

  it should "honour the bucket names and the local overrides shared with the Python side" in {
    LakeSettings.from(AppConfig.defaults("MINIO_BUCKET_GOLD = otro-gold")).goldRoot shouldBe "s3a://otro-gold"

    val lake = LakeSettings.from(AppConfig.defaults(
      "SILVER_ROOT = \"file:///tmp/silver\"\nGOLD_ROOT = \"file:///tmp/gold\"\nTRAZABILIDAD_ROOT = \"file:///tmp/traza\""
    ))
    lake.silverRoot shouldBe "file:///tmp/silver"
    lake.goldRoot shouldBe "file:///tmp/gold"
    lake.trazabilidadRoot shouldBe "file:///tmp/traza"
    lake.cargasDir shouldBe "file:///tmp/traza/cargas/"
  }
}
