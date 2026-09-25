package raillytics.common.lake

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LakePathsSpec extends AnyFlatSpec with Matchers {

  "LakePaths" should "default to the MinIO buckets with the s3a scheme" in {
    LakePaths.silverRoot(Map.empty) shouldBe "s3a://raillytics-silver"
    LakePaths.goldRoot(Map.empty) shouldBe "s3a://raillytics-gold"
    LakePaths.trazabilidadRoot(Map.empty) shouldBe "s3a://raillytics-gold/_trazabilidad"
    LakePaths.cargasDir(LakePaths.trazabilidadRoot(Map.empty)) shouldBe "s3a://raillytics-gold/_trazabilidad/cargas/"
    LakePaths.goldTable("s3a://raillytics-gold", "dim_fecha") shouldBe "s3a://raillytics-gold/dim_fecha/"
    LakePaths.silverTable("s3a://raillytics-silver", "viajeros_enriquecidos") shouldBe "s3a://raillytics-silver/viajeros_enriquecidos/"
  }

  it should "honour the bucket names and the local overrides shared with the Python side" in {
    LakePaths.goldRoot(Map("MINIO_BUCKET_GOLD" -> "otro-gold")) shouldBe "s3a://otro-gold"
    val env = Map("SILVER_ROOT" -> "file:///tmp/silver", "GOLD_ROOT" -> "file:///tmp/gold", "TRAZABILIDAD_ROOT" -> "file:///tmp/traza")
    LakePaths.silverRoot(env) shouldBe "file:///tmp/silver"
    LakePaths.goldRoot(env) shouldBe "file:///tmp/gold"
    LakePaths.trazabilidadRoot(env) shouldBe "file:///tmp/traza"
  }
}
