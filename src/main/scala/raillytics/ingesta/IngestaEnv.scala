package raillytics.ingesta

import raillytics.common.lake.LakePaths

// Defaults de las variables de entorno que comparten L1 (raw-uploader) y L2
// (parquet-converter): el directorio "done" de L1 es la entrada de L2, así
// que ambos deben resolverlo igual. Lo específico de cada app vive en su
// propio *Settings (l1/RawUploaderSettings, l2/ParquetConverterSettings).
object IngestaEnv {
  // Ficheros ya subidos a MinIO por L1 y pendientes de L2. Es HERMANO de
  // data/bronze, no un subdirectorio: así el glob de L1 no los vuelve a ver.
  def l1DoneRoot(env: Map[String, String]): String =
    env.getOrElse("L1_DONE_ROOT", "data/bronze_l1_done")

  // Checkpoints de Structured Streaming (uno por query): recuerdan qué ficheros
  // se procesaron ya. Borrarlos hace que las apps reprocesen todo lo que encuentren.
  def checkpointRoot(env: Map[String, String]): String =
    env.getOrElse("CHECKPOINT_ROOT", "data/checkpoints")

  // Bucket Bronze, siempre en MinIO (esquema s3a://, el de Hadoop).
  def bronzeRoot(env: Map[String, String]): String =
    s"s3a://${env.getOrElse("MINIO_BUCKET_BRONZE", "raillytics-bronze")}"

  // Registro de cargas compartido con Gold y con el lado Python (raillytics.common.trazabilidad.Cargas).
  def cargasDir(env: Map[String, String]): String =
    LakePaths.cargasDir(LakePaths.trazabilidadRoot(env))
}
