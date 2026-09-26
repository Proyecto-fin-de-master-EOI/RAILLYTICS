package raillytics.ingesta.l1

import com.typesafe.config.Config
import raillytics.common.config.AppConfig
import raillytics.common.lake.LakeSettings

// Rutas de L1: claves raillytics.ingesta.* y raillytics.lake.* de application.conf.
final case class RawUploaderSettings(
  stagingRoot: String,     // data/bronze: donde deja los ficheros la descarga Python, por fuente
  l1DoneRoot: String,      // a dónde se mueven una vez subidos (es la entrada de L2)
  checkpointRoot: String,  // checkpoints de la query de streaming
  bronzeRoot: String,      // bucket Bronze en MinIO (s3a://)
  cargasDir: String,       // registro de cargas (trazabilidad)
  calidadDir: String       // registro de quality gates (trazabilidad)
)

object RawUploaderSettings {
  // config es un parámetro (y no AppConfig.load() a secas) para poder probarlo
  // con AppConfig.defaults(...) sin depender del entorno.
  def from(config: Config = AppConfig.load()): RawUploaderSettings = {
    val ingesta = config.getConfig("raillytics.ingesta")
    val lake = LakeSettings.from(config)
    RawUploaderSettings(
      stagingRoot = ingesta.getString("staging-root"),
      l1DoneRoot = ingesta.getString("l1-done-root"),
      checkpointRoot = ingesta.getString("checkpoint-root"),
      bronzeRoot = lake.bronzeRoot,
      cargasDir = lake.cargasDir,
      calidadDir = lake.calidadDir
    )
  }
}
