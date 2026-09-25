package raillytics.ingesta.l1

import raillytics.ingesta.IngestaEnv

final case class RawUploaderSettings(
  stagingRoot: String,
  l1DoneRoot: String,
  checkpointRoot: String,
  bronzeRoot: String
)

object RawUploaderSettings {
  def fromEnv(env: Map[String, String] = sys.env): RawUploaderSettings =
    RawUploaderSettings(
      stagingRoot = env.getOrElse("STAGING_ROOT", "data/bronze"),
      l1DoneRoot = IngestaEnv.l1DoneRoot(env),
      checkpointRoot = IngestaEnv.checkpointRoot(env),
      bronzeRoot = IngestaEnv.bronzeRoot(env)
    )
}
