package raillytics.ingesta.l1

import raillytics.ingesta.IngestaEnv

// Rutas de L1; todas salen del entorno (fromEnv), con defaults pensados para
// ejecutar desde la raíz del repo.
final case class RawUploaderSettings(
  stagingRoot: String,     // data/bronze: donde deja los ficheros la descarga Python, por fuente
  l1DoneRoot: String,      // a dónde se mueven una vez subidos (es la entrada de L2)
  checkpointRoot: String,  // checkpoints de la query de streaming
  bronzeRoot: String,      // bucket Bronze en MinIO (s3a://)
  cargasDir: String        // registro de cargas (trazabilidad)
)

object RawUploaderSettings {
  // env es un parámetro (y no sys.env directamente) para poder probarlo con mapas fijos.
  def fromEnv(env: Map[String, String] = sys.env): RawUploaderSettings =
    RawUploaderSettings(
      stagingRoot = env.getOrElse("STAGING_ROOT", "data/bronze"),
      l1DoneRoot = IngestaEnv.l1DoneRoot(env),
      checkpointRoot = IngestaEnv.checkpointRoot(env),
      bronzeRoot = IngestaEnv.bronzeRoot(env),
      cargasDir = IngestaEnv.cargasDir(env)
    )
}
