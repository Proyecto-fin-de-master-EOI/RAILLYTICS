package raillytics.gold

import raillytics.common.lake.LakePaths

final case class GoldBuilderSettings(
  silverRoot: String,
  goldRoot: String,
  cargasDir: String
)

object GoldBuilderSettings {
  def fromEnv(env: Map[String, String] = sys.env): GoldBuilderSettings =
    GoldBuilderSettings(
      silverRoot = LakePaths.silverRoot(env),
      goldRoot = LakePaths.goldRoot(env),
      cargasDir = LakePaths.cargasDir(LakePaths.trazabilidadRoot(env))
    )
}
