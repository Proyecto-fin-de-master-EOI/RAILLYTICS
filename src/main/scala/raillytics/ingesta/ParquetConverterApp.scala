package raillytics.ingesta

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.streaming.StreamingQuery

import java.net.URI
import java.time.LocalDate
import scala.util.{Failure, Success, Try}

object ParquetConverterApp {

  // Columna de bookkeeping (no analítica): readStream sobre csv/json no expone
  // "path" como sí hace binaryFile, así que hace falta input_file_name() para
  // saber qué ficheros componen cada micro-batch y poder moverlos después.
  val SourceFileCol = "_source_file"

  def moveProcessedFiles(filePaths: Seq[String], hadoopConf: Configuration, source: String, processedRoot: String): Unit = {
    val localFs = FileSystem.get(new URI("file:///"), hadoopConf)
    val destDir = new Path(s"$processedRoot/$source")
    localFs.mkdirs(destDir)

    filePaths.foreach { localUri =>
      val srcPath = new Path(localUri)
      localFs.rename(srcPath, new Path(destDir, srcPath.getName))
    }
  }

  def processBatch(source: DataSource, batch: DataFrame, hadoopConf: Configuration, bronzeRoot: String, processedRoot: String): Unit = {
    val filesInBatch = batch.select(SourceFileCol).distinct().collect().map(_.getString(0)).toSeq

    batch.drop(SourceFileCol).write.mode("append")
      .parquet(BronzePaths.l2(bronzeRoot, source.id, LocalDate.now()))

    moveProcessedFiles(filesInBatch, hadoopConf, source.id, processedRoot)
  }

  def startQuery(source: DataSource, bronzeRoot: String, l1DoneRoot: String, processedRoot: String,
                 checkpointRoot: String, hadoopConf: Configuration)
                (implicit spark: SparkSession): StreamingQuery = {
    spark.readStream
      .format(source.format)               // "csv" | "json", desde el YAML
      .option("header", "true")
      .load(s"$l1DoneRoot/${source.id}/*")  // plano, sin recursión (hermano de data/bronze/)
      .withColumn(SourceFileCol, input_file_name())
      .writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        processBatch(source, batch, hadoopConf, bronzeRoot, processedRoot)
      }
      .option("checkpointLocation", s"$checkpointRoot/l2/${source.id}")
      .start()
  }

  def main(args: Array[String]): Unit = {
    val configPath = sys.env.getOrElse("DATA_SOURCES_CONFIG", "config/data_sources.yml")
    val l1DoneRoot = sys.env.getOrElse("L1_DONE_ROOT", "data/bronze_l1_done")
    val processedRoot = sys.env.getOrElse("PROCESSED_ROOT", "data/bronze_processed")
    val checkpointRoot = sys.env.getOrElse("CHECKPOINT_ROOT", "data/checkpoints")
    val bronzeRoot = s"s3a://${sys.env.getOrElse("MINIO_BUCKET_BRONZE", "raillytics-bronze")}"

    implicit val spark: SparkSession = SparkSessionFactory.build("parquet-converter")
    // Igual que en RawUploaderApp: readStream sobre csv/json también exige inferencia
    // de esquema (o un esquema explícito) antes de resolver la fuente.
    spark.conf.set("spark.sql.streaming.schemaInference", "true")
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Cada fuente arranca su query de forma independiente: si el directorio
    // L1 de una fuente está vacío (su estado normal en reposo, o cualquier
    // fuente recién registrada sin su primer fichero L1 todavía),
    // schemaInference lanza una AnalysisException síncrona dentro del
    // .foreach -- sin el Try, eso tumbaría main() entero y ninguna otra
    // fuente (aunque esté sana) llegaría a arrancar su query.
    DataSourceConfig.load(configPath).foreach { source =>
      Try(startQuery(source, bronzeRoot, l1DoneRoot, processedRoot, checkpointRoot, hadoopConf)) match {
        case Success(_) =>
          println(s"[parquet-converter] query iniciada para la fuente '${source.id}'")
        case Failure(e) =>
          println(s"[parquet-converter] WARN: no se pudo iniciar la query para '${source.id}': ${e.getMessage}")
      }
    }

    // Si una query muere (p.ej. fichero mal formado), todo el proceso termina en
    // vez de quedarse a medias con unas fuentes vivas y otras muertas en silencio.
    spark.streams.awaitAnyTermination()
  }
}
