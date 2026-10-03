package raillytics.silver

import com.typesafe.config.Config
import raillytics.common.config.AppConfig
import raillytics.common.lake.LakeSettings

// Rutas y tiempos de `make 04_silver`: claves raillytics.silver.*, raillytics.ingesta.* y raillytics.lake.* de application.conf.
final case class SilverSettings(
  configPath: String,        // config/data_sources.yml: qué tablas Silver construye cada fuente
  calidadConfig: String,     // config/quality_gates.yml: los gates silver_<tabla>
  checkpointRoot: String,    // checkpoints de streaming (uno por tabla, bajo silver/)
  lake: LakeSettings,        // raíces de Bronze, Silver, Gold y trazabilidad
  triggerIntervalMs: Long,   // cada cuánto mira si L2 ha dejado ficheros nuevos
  pendingRetryMs: Long       // cada cuánto se reintenta arrancar las tablas cuya fuente aún no tiene ficheros en L2
) {
  def bronzeRoot: String = lake.bronzeRoot
  def cargasDir: String = lake.cargasDir
  def calidadDir: String = lake.calidadDir
}

object SilverSettings {
  // config es un parámetro (y no AppConfig.load() a secas) para poder probarlo con AppConfig.defaults(...).
  def from(config: Config = AppConfig.load()): SilverSettings = {
    val ingesta = config.getConfig("raillytics.ingesta")
    val silver = config.getConfig("raillytics.silver")
    SilverSettings(
      configPath = ingesta.getString("data-sources"),
      calidadConfig = config.getString("raillytics.calidad.config"),
      checkpointRoot = ingesta.getString("checkpoint-root"),
      lake = LakeSettings.from(config),
      triggerIntervalMs = silver.getDuration("trigger-interval").toMillis,
      pendingRetryMs = silver.getDuration("pending-retry").toMillis
    )
  }
}
