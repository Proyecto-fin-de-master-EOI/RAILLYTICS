package raillytics.ingesta

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def build(appName: String): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.hadoop.fs.s3a.endpoint", sys.env.getOrElse("MINIO_ENDPOINT", ""))
      .config("spark.hadoop.fs.s3a.access.key", sys.env.getOrElse("MINIO_ROOT_USER", ""))
      .config("spark.hadoop.fs.s3a.secret.key", sys.env.getOrElse("MINIO_ROOT_PASSWORD", ""))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()
}
