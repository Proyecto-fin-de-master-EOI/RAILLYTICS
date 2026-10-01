package raillytics.ingesta.l2

import com.typesafe.config.Config
import raillytics.common.config.AppConfig
import raillytics.common.lake.LakeSettings

// Rutas y parámetros de L2: claves raillytics.ingesta.* y raillytics.lake.* de application.conf.
final case class ParquetConverterSettings(
  configPath: String,      // config/data_sources.yml: una query por fuente
  l1DoneRoot: String,      // entrada: lo que L1 ya subió a MinIO (por fuente)
  processedRoot: String,   // a dónde se mueven los ficheros ya convertidos a Parquet
  rejectedRoot: String,    // cuarentena: ficheros que no pasan los quality gates de L2 (por fuente)
  checkpointRoot: String,  // checkpoints de streaming (uno por fuente, bajo l2/)
  bronzeRoot: String,      // bucket Bronze en MinIO (s3a://)
  cargasDir: String,       // registro de cargas (trazabilidad)
  calidadDir: String,      // registro de quality gates (trazabilidad)
  pendingRetryMs: Long     // cada cuánto se reintenta arrancar las fuentes aún sin ficheros en L1
)

object ParquetConverterSettings {
  // config es un parámetro (y no AppConfig.load() a secas) para poder probarlo
  // con AppConfig.defaults(...) sin depender del entorno.
  def from(config: Config = AppConfig.load()): ParquetConverterSettings = {
    val ingesta = config.getConfig("raillytics.ingesta")
    val lake = LakeSettings.from(config)
    ParquetConverterSettings(
      configPath = ingesta.getString("data-sources"),
      l1DoneRoot = ingesta.getString("l1-done-root"),
      processedRoot = ingesta.getString("processed-root"),
      rejectedRoot = ingesta.getString("rejected-root"),
      checkpointRoot = ingesta.getString("checkpoint-root"),
      bronzeRoot = lake.bronzeRoot,
      cargasDir = lake.cargasDir,
      calidadDir = lake.calidadDir,
      pendingRetryMs = ingesta.getDuration("l2.pending-retry").toMillis
    )
  }
}
