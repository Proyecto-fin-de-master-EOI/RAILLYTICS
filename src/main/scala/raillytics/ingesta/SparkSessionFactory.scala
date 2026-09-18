package raillytics.ingesta

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkSessionFactory {
  private val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

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
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()
  }
}
