package raillytics.common.lake

// Raíces de Silver, Gold y trazabilidad, resueltas del entorno con las mismas
// variables que el lado Python (python/raillytics/utils/lake.py): por defecto
// los buckets MINIO_BUCKET_* (con esquema s3a://, el de Hadoop/Spark), y
// SILVER_ROOT / GOLD_ROOT / TRAZABILIDAD_ROOT para apuntar a directorios
// locales (tests, desarrollo sin MinIO). Lo de Bronze sigue en BronzePaths.
object LakePaths {
  // Bucket Silver, o el directorio local de SILVER_ROOT si está definido.
  def silverRoot(env: Map[String, String]): String =
    env.getOrElse("SILVER_ROOT", s"s3a://${env.getOrElse("MINIO_BUCKET_SILVER", "raillytics-silver")}")

  // Bucket Gold, o el directorio local de GOLD_ROOT si está definido.
  def goldRoot(env: Map[String, String]): String =
    env.getOrElse("GOLD_ROOT", s"s3a://${env.getOrElse("MINIO_BUCKET_GOLD", "raillytics-gold")}")

  // Raíz del registro de cargas: por defecto un prefijo "técnico" dentro de Gold.
  // El guion bajo inicial hace que Spark lo ignore si alguien lee el bucket entero.
  def trazabilidadRoot(env: Map[String, String]): String =
    env.getOrElse("TRAZABILIDAD_ROOT", s"${goldRoot(env)}/_trazabilidad")

  // Cada tabla vive en su propio prefijo: Spark lee y escribe el directorio
  // entero (part-*.parquet), y DuckDB/Superset lo leen con <tabla>/*.parquet.
  def silverTable(silverRoot: String, table: String): String = s"$silverRoot/$table/"

  def goldTable(goldRoot: String, table: String): String = s"$goldRoot/$table/"

  // Registro de cargas (raillytics.common.trazabilidad.Cargas).
  def cargasDir(trazabilidadRoot: String): String = s"$trazabilidadRoot/cargas/"
}
