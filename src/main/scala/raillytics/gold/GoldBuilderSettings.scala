package raillytics.gold

import raillytics.common.lake.LakePaths

// Rutas con las que trabaja GoldBuilder; todas salen del entorno vía LakePaths.
final case class GoldBuilderSettings(
  silverRoot: String,   // de dónde se leen las tablas Silver (bucket o directorio local)
  goldRoot: String,     // dónde se escriben las tablas Gold
  cargasDir: String     // dónde se registra la ejecución (trazabilidad de cargas)
)

object GoldBuilderSettings {
  // env es un parámetro (y no sys.env directamente) para poder probarlo con mapas fijos.
  def fromEnv(env: Map[String, String] = sys.env): GoldBuilderSettings =
    GoldBuilderSettings(
      silverRoot = LakePaths.silverRoot(env),
      goldRoot = LakePaths.goldRoot(env),
      cargasDir = LakePaths.cargasDir(LakePaths.trazabilidadRoot(env))
    )
}
