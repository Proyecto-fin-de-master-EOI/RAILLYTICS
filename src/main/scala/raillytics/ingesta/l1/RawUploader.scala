package raillytics.ingesta.l1

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import raillytics.common.fs.HadoopFs
import raillytics.common.lake.BronzePaths
import raillytics.common.logging.Logging
import raillytics.common.trazabilidad.Cargas

import java.net.URI
import java.nio.file.Paths
import java.time.LocalDate

// L1: copia cada fichero del staging local tal cual a MinIO (l1-raw) y lo
// mueve a l1DoneRoot, de donde lo recoge L2.
object RawUploader extends Logging {

  // binaryFile entrega cada ruta como URI (file:///.../data/bronze/<fuente>/<fichero>):
  // la fuente es el nombre de la carpeta padre, que es como la organiza la descarga Python.
  private def sourceIdFromLocalPath(uriString: String): String =
    Paths.get(new URI(uriString)).getParent.getFileName.toString

  // Nombre del fichero tal cual lo dejó Python (con su timestamp de descarga por delante).
  private def fileNameFromLocalPath(uriString: String): String =
    Paths.get(new URI(uriString)).getFileName.toString

  // Procesa un micro-batch de la fuente binaryFile. Solo se usan las columnas
  // path y length: el contenido no pasa por Spark, la copia la hace Hadoop
  // (FileUtil.copy) fichero a fichero, del disco local a MinIO.
  def processBatch(batch: DataFrame, hadoopConf: Configuration, bronzeRoot: String, l1DoneRoot: String,
                   cargasDir: String): Unit = {
    implicit val spark: SparkSession = batch.sparkSession
    val localFs = HadoopFs.local(hadoopConf)
    val destFs = HadoopFs.forRoot(bronzeRoot, hadoopConf)

    val files = batch.select("path", "length").collect().map(row => (row.getString(0), row.getLong(1)))
    logger.info(s"micro-batch de ${files.length} fichero(s) recibido")
    // Spark puede entregar un micro-batch vacío: no hay nada que subir ni que registrar.
    if (files.isEmpty) return

    // Trazabilidad: una fila por fichero subido (tabla = fuente, bytes = tamaño).
    Cargas.registrar("bronze_l1_raw_uploader", "bronze", cargasDir, Map("ficheros" -> files.length)) { ejecucion =>
      files.foreach { case (localUri, length) =>
        val source = sourceIdFromLocalPath(localUri)
        val fileName = fileNameFromLocalPath(localUri)

        val srcPath = new Path(localUri)
        val destUri = BronzePaths.l1(bronzeRoot, source, LocalDate.now(), fileName)
        val destPath = new Path(destUri)
        ejecucion.tabla(source, origen = Some(localUri), destino = Some(destUri)) { carga =>
          logger.debug(s"copiando '$srcPath' -> '$destPath' (fuente=$source)")
          FileUtil.copy(localFs, srcPath, destFs, destPath, false, hadoopConf)

          // l1DoneRoot es HERMANO del staging (no subdirectorio) — un subdirectorio
          // anidado sería recogido de nuevo por el glob de esta misma query.
          HadoopFs.moveInto(localFs, srcPath, new Path(s"$l1DoneRoot/$source"), fileName)
          carga.bytes = Some(length)
        }
      }
    }
  }

  // Query de streaming sobre el staging. binaryFile detecta ficheros nuevos por
  // su ruta (no por contenido: un fichero reescrito con el mismo nombre no se
  // vuelve a subir) y el checkpoint recuerda los ya vistos entre reinicios.
  def startQuery(settings: RawUploaderSettings, hadoopConf: Configuration)
                (implicit spark: SparkSession): StreamingQuery =
    spark.readStream
      .format("binaryFile")
      .load(s"${settings.stagingRoot}/*/*")   // data/bronze/<source>/<file> (plano, sin recursión)
      .writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        processBatch(batch, hadoopConf, settings.bronzeRoot, settings.l1DoneRoot, settings.cargasDir)
      }
      .option("checkpointLocation", s"${settings.checkpointRoot}/l1-raw")
      .start()
}
