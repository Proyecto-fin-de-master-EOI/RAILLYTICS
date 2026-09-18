package raillytics.ingesta

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.DataFrame

import java.net.URI
import java.nio.file.Paths
import java.time.LocalDate

object RawUploaderApp {

  def sourceIdFromLocalPath(uriString: String): String =
    Paths.get(new URI(uriString)).getParent.getFileName.toString

  def fileNameFromLocalPath(uriString: String): String =
    Paths.get(new URI(uriString)).getFileName.toString

  def processBatch(batch: DataFrame, hadoopConf: Configuration, bronzeRoot: String, l1DoneRoot: String): Unit = {
    val localFs = FileSystem.get(new URI("file:///"), hadoopConf)
    val destFs = FileSystem.get(new URI(bronzeRoot), hadoopConf)

    batch.select("path").collect().foreach { row =>
      val localUri = row.getString(0)
      val source = sourceIdFromLocalPath(localUri)
      val fileName = fileNameFromLocalPath(localUri)

      val srcPath = new Path(localUri)
      val destPath = new Path(BronzePaths.l1(bronzeRoot, source, LocalDate.now(), fileName))
      FileUtil.copy(localFs, srcPath, destFs, destPath, false, hadoopConf)

      // l1DoneRoot es HERMANO del staging (no subdirectorio) — un subdirectorio
      // anidado sería recogido de nuevo por el glob de esta misma query.
      val doneDir = new Path(s"$l1DoneRoot/$source")
      localFs.mkdirs(doneDir)
      localFs.rename(srcPath, new Path(doneDir, fileName))
    }
  }

  def main(args: Array[String]): Unit = {
    val stagingRoot = sys.env.getOrElse("STAGING_ROOT", "data/bronze")
    val l1DoneRoot = sys.env.getOrElse("L1_DONE_ROOT", "data/bronze_l1_done")
    val checkpointRoot = sys.env.getOrElse("CHECKPOINT_ROOT", "data/checkpoints")
    val bronzeRoot = s"s3a://${sys.env.getOrElse("MINIO_BUCKET_BRONZE", "raillytics-bronze")}"

    val spark = SparkSessionFactory.build("raw-uploader")
    // El esquema de binaryFile es fijo (path, modificationTime, length, content), pero Spark
    // exige igualmente habilitar la inferencia de esquema en streaming o pasar uno explícito.
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    val query = spark.readStream
      .format("binaryFile")
      .load(s"$stagingRoot/*/*")              // data/bronze/<source>/<file> (plano, sin recursión)
      .writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        processBatch(batch, hadoopConf, bronzeRoot, l1DoneRoot)
      }
      .option("checkpointLocation", s"$checkpointRoot/l1-raw")
      .start()

    query.awaitTermination()
  }
}
