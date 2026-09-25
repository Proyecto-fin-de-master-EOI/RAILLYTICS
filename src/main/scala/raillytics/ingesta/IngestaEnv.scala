package raillytics.ingesta

// Defaults de las variables de entorno que comparten L1 (raw-uploader) y L2
// (parquet-converter): el directorio "done" de L1 es la entrada de L2, así
// que ambos deben resolverlo igual. Lo específico de cada app vive en su
// propio *Settings (l1/RawUploaderSettings, l2/ParquetConverterSettings).
object IngestaEnv {
  def l1DoneRoot(env: Map[String, String]): String =
    env.getOrElse("L1_DONE_ROOT", "data/bronze_l1_done")

  def checkpointRoot(env: Map[String, String]): String =
    env.getOrElse("CHECKPOINT_ROOT", "data/checkpoints")

  def bronzeRoot(env: Map[String, String]): String =
    s"s3a://${env.getOrElse("MINIO_BUCKET_BRONZE", "raillytics-bronze")}"
}
