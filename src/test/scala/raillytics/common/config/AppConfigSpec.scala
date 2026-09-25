package raillytics.common.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.common.lake.LakeSettings

import java.nio.file.{Files, Path}

class AppConfigSpec extends AnyFlatSpec with Matchers {

  // Clave que no está en el .env del repo ni en el entorno habitual: así el test
  // no depende de la máquina en la que corre.
  private val Clave = "TRAZABILIDAD_ROOT"

  private def dotEnv(contenido: String): Path = {
    val file = Files.createTempDirectory("appconfig-spec").resolve(".env")
    Files.writeString(file, contenido)
    file
  }

  "AppConfig.defaults" should "expose the application.conf defaults without touching the environment" in {
    val lake = LakeSettings.from(AppConfig.defaults())
    lake shouldBe LakeSettings(
      bronzeRoot = "s3a://raillytics-bronze",
      silverRoot = "s3a://raillytics-silver",
      goldRoot = "s3a://raillytics-gold",
      trazabilidadRoot = "s3a://raillytics-gold/_trazabilidad"
    )
    AppConfig.defaults().getString("raillytics.minio.endpoint") shouldBe "http://localhost:9000"
    AppConfig.defaults().getInt("raillytics.gold.umbral-puntualidad-min") shouldBe 5
  }

  it should "apply HOCON overrides as if they were environment variables" in {
    val config = AppConfig.defaults("MINIO_BUCKET_GOLD = otro-gold\nSILVER_ROOT = \"file:///tmp/silver\"")
    val lake = LakeSettings.from(config)
    lake.goldRoot shouldBe "s3a://otro-gold"
    lake.trazabilidadRoot shouldBe "s3a://otro-gold/_trazabilidad"  // derivada del nuevo bucket
    lake.silverRoot shouldBe "file:///tmp/silver"
  }

  "AppConfig.load" should "read the project .env when the variable is not in the environment" in {
    val config = AppConfig.load(dotEnv(s"# comentario\n$Clave=file:///tmp/traza\nOTRA=\"con comillas\"\n"))
    config.getString("raillytics.lake.trazabilidad-root") shouldBe "file:///tmp/traza"
    config.getString("OTRA") shouldBe "con comillas"
  }

  it should "let a JVM system property win over the .env" in {
    System.setProperty(Clave, "file:///tmp/propiedad")
    try {
      val config = AppConfig.load(dotEnv(s"$Clave=file:///tmp/traza\n"))
      config.getString("raillytics.lake.trazabilidad-root") shouldBe "file:///tmp/propiedad"
    } finally System.clearProperty(Clave)
  }

  it should "fall back to the defaults when there is no .env" in {
    val config = AppConfig.load(Files.createTempDirectory("appconfig-spec").resolve("no-existe.env"))
    config.getString("raillytics.ingesta.staging-root") shouldBe "data/bronze"
  }
}
