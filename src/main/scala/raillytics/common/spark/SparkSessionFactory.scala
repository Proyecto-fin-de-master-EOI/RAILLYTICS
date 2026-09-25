package raillytics.common.spark

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import raillytics.common.config.AppConfig
import raillytics.common.logging.Logging

// Crea las SparkSession de las apps con el acceso a MinIO (S3A) ya configurado
// a partir de las claves raillytics.spark.* y raillytics.minio.* de
// application.conf (que a su vez salen de las variables MINIO_* del .env).
// Todas las apps (L1, L2, Gold) pasan por aquí para hablar con los buckets
// de la misma manera.
object SparkSessionFactory extends Logging {

  def build(appName: String, config: Config = AppConfig.load()): SparkSession = {
    val sparkConf = config.getConfig("raillytics.spark")
    val minio = config.getConfig("raillytics.minio")
    val master = sparkConf.getString("master")
    val endpoint = minio.getString("endpoint")
    val user = minio.getString("user")
    val password = minio.getString("password")

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
      // Modo path-style y TLS: ver los comentarios de raillytics.spark.s3a en application.conf.
      .config("spark.hadoop.fs.s3a.path.style.access", sparkConf.getBoolean("s3a.path-style-access").toString)
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", sparkConf.getBoolean("s3a.ssl-enabled").toString)
      .getOrCreate()
  }

  // readStream sobre ficheros (binaryFile, csv, json...) exige un esquema
  // explícito o habilitar la inferencia antes de resolver la fuente -- incluso
  // binaryFile, cuyo esquema es fijo (path, modificationTime, length, content).
  def buildForFileStreaming(appName: String, config: Config = AppConfig.load()): SparkSession = {
    val spark = build(appName, config)
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
    spark
  }
}
