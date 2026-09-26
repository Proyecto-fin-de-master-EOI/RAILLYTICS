package raillytics.calidad

import com.typesafe.config.Config
import raillytics.common.config.AppConfig
import raillytics.common.lake.LakeSettings

// Lo que necesita QualityGatesApp: las raíces del lake y el YAML de gates.
final case class QualityGatesSettings(
  lake: LakeSettings,
  calidadConfig: String   // config/quality_gates.yml
)

object QualityGatesSettings {
  def from(config: Config = AppConfig.load()): QualityGatesSettings =
    QualityGatesSettings(
      lake = LakeSettings.from(config),
      calidadConfig = config.getString("raillytics.calidad.config")
    )
}
