package raillytics.ingesta.l2

import raillytics.ingesta.IngestaEnv

final case class ParquetConverterSettings(
  configPath: String,
  l1DoneRoot: String,
  processedRoot: String,
  checkpointRoot: String,
  bronzeRoot: String,
  cargasDir: String
)

object ParquetConverterSettings {
  def fromEnv(env: Map[String, String] = sys.env): ParquetConverterSettings =
    ParquetConverterSettings(
      configPath = env.getOrElse("DATA_SOURCES_CONFIG", "config/data_sources.yml"),
      l1DoneRoot = IngestaEnv.l1DoneRoot(env),
      processedRoot = env.getOrElse("PROCESSED_ROOT", "data/bronze_processed"),
      checkpointRoot = IngestaEnv.checkpointRoot(env),
      bronzeRoot = IngestaEnv.bronzeRoot(env),
      cargasDir = IngestaEnv.cargasDir(env)
    )
}
