package raillytics.common.spark

import org.apache.spark.sql.SparkSession
import raillytics.common.logging.Logging

object SparkSessionFactory extends Logging {

  def build(appName: String): SparkSession = {
    val master = sys.env.getOrElse("SPARK_MASTER", "local[*]")
    val endpoint = sys.env.getOrElse("MINIO_ENDPOINT", "")
    val user = sys.env.getOrElse("MINIO_ROOT_USER", "")
    val password = sys.env.getOrElse("MINIO_ROOT_PASSWORD", "")

    logger.info(s"creando SparkSession '$appName' (master=$master, s3a.endpoint=$endpoint)")
    // Nunca se loguean los valores de usuario/contraseña, solo si están
    // presentes -- unas credenciales ausentes son un fallo de configuración
    // que interesa detectar pronto, pero su valor no debe acabar en un log.
    if (user.isEmpty || password.isEmpty) {
      logger.warn("credenciales S3A (MINIO_ROOT_USER/MINIO_ROOT_PASSWORD) ausentes o vacías")
    }

    SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.hadoop.fs.s3a.endpoint", endpoint)
      .config("spark.hadoop.fs.s3a.access.key", user)
      .config("spark.hadoop.fs.s3a.secret.key", password)
      // MinIO expone los buckets como rutas (http://host:puerto/bucket), no como
      // subdominios (bucket.host), que es lo que S3A asume por defecto.
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      // El MinIO de docker-compose va por http, sin TLS.
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()
  }

  // readStream sobre ficheros (binaryFile, csv, json...) exige un esquema
  // explícito o habilitar la inferencia antes de resolver la fuente -- incluso
  // binaryFile, cuyo esquema es fijo (path, modificationTime, length, content).
  def buildForFileStreaming(appName: String): SparkSession = {
    val spark = build(appName)
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
    spark
  }
}
