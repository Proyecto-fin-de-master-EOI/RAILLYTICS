package raillytics.gold

import com.typesafe.config.Config
import raillytics.common.config.AppConfig
import raillytics.common.lake.LakeSettings

// Lo que necesita GoldBuilder: las raíces del lake (de dónde lee Silver, dónde
// escribe Gold y dónde registra la ejecución) y los parámetros del modelo.
final case class GoldBuilderSettings(
  lake: LakeSettings,
  umbralPuntualidadMin: Int   // un servicio es puntual si llega con este retraso o menos (minutos)
)

object GoldBuilderSettings {
  // config es un parámetro (y no AppConfig.load() a secas) para poder probarlo
  // con AppConfig.defaults(...) sin depender del entorno.
  def from(config: Config = AppConfig.load()): GoldBuilderSettings =
    GoldBuilderSettings(
      lake = LakeSettings.from(config),
      umbralPuntualidadMin = config.getInt("raillytics.gold.umbral-puntualidad-min")
    )
}
