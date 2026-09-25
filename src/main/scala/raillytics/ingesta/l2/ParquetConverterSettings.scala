package raillytics.ingesta.l2

import raillytics.ingesta.IngestaEnv

// Rutas de L2; todas salen del entorno (fromEnv), con defaults pensados para
// ejecutar desde la raíz del repo.
final case class ParquetConverterSettings(
  configPath: String,      // config/data_sources.yml: una query por fuente
  l1DoneRoot: String,      // entrada: lo que L1 ya subió a MinIO (por fuente)
  processedRoot: String,   // a dónde se mueven los ficheros ya convertidos a Parquet
  checkpointRoot: String,  // checkpoints de streaming (uno por fuente, bajo l2/)
  bronzeRoot: String,      // bucket Bronze en MinIO (s3a://)
  cargasDir: String        // registro de cargas (trazabilidad)
)

object ParquetConverterSettings {
  // env es un parámetro (y no sys.env directamente) para poder probarlo con mapas fijos.
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
