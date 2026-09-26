package raillytics.ingesta.l1

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import raillytics.common.calidad.QualityGates
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

  private val Proceso = "bronze_l1_raw_uploader"

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
  //
  // Es idempotente frente a la repetición de un micro-batch tras un reinicio (ver
  // SparkSessionFactory.buildForFileStreaming): los ficheros que ya se movieron a
  // l1DoneRoot se omiten y los que siguen en el staging se vuelven a copiar
  // (sobrescribiendo) y a mover. Antes, un fallo a mitad de batch dejaba la query
  // muerta en cada arranque: al reintentar, la copia de los ya subidos fallaba con
  // "already exists" y la de los ya movidos con FileNotFoundException.
  def processBatch(batch: DataFrame, hadoopConf: Configuration, settings: RawUploaderSettings): Unit = {
    implicit val spark: SparkSession = batch.sparkSession
    val localFs = HadoopFs.local(hadoopConf)
    val destFs = HadoopFs.forRoot(settings.bronzeRoot, hadoopConf)

    val files = batch.select("path", "length").collect().map(row => (row.getString(0), row.getLong(1)))
    logger.info(s"micro-batch de ${files.length} fichero(s) recibido")
    // Spark puede entregar un micro-batch vacío: no hay nada que subir ni que registrar.
    if (files.isEmpty) return

    val pendientes = files.filter { case (uri, _) => localFs.exists(new Path(uri)) }
    if (pendientes.length < files.length) {
      logger.info(s"${files.length - pendientes.length} fichero(s) del batch ya subidos en una ejecución anterior: se omiten")
    }
    if (pendientes.isEmpty) return

    // Trazabilidad: una fila por fichero subido (tabla = fuente, bytes = tamaño) y
    // un quality gate por fichero (bytes_subidos), con el mismo run_id.
    val gates = new QualityGates.Acumulador
    Cargas.registrar(Proceso, "bronze", settings.cargasDir, Map("ficheros" -> pendientes.length)) { ejecucion =>
      try pendientes.foreach { case (localUri, length) =>
        val source = sourceIdFromLocalPath(localUri)
        val fileName = fileNameFromLocalPath(localUri)

        val srcPath = new Path(localUri)
        val destUri = BronzePaths.l1(settings.bronzeRoot, source, LocalDate.now(), fileName)
        val destPath = new Path(destUri)
        ejecucion.tabla(source, origen = Some(localUri), destino = Some(destUri)) { carga =>
          logger.debug(s"copiando '$srcPath' -> '$destPath' (fuente=$source)")
          FileUtil.copy(localFs, srcPath, destFs, destPath, false, true, hadoopConf)

          // Quality gate: lo que ha quedado en Bronze pesa exactamente lo mismo que el
          // fichero local. Si no, se borra del destino y el batch falla: al reintentarlo
          // (mismo batch tras reiniciar) se vuelve a subir.
          val subidos = destFs.getFileStatus(destPath).getLen
          val gate = gates += QualityGates.resultado(
            source, "bytes_subidos", QualityGates.Bloqueante, subidos == length, subidos.toDouble, s"= $length",
            Some(s"$fileName: $subidos bytes en destino frente a $length en origen")
          )
          if (!gate.pasa) {
            destFs.delete(destPath, false)
            throw new QualityGates.QualityGateException(Seq(gate))
          }

          // l1DoneRoot es HERMANO del staging (no subdirectorio) — un subdirectorio
          // anidado sería recogido de nuevo por el glob de esta misma query.
          HadoopFs.moveInto(localFs, srcPath, new Path(s"${settings.l1DoneRoot}/$source"), fileName)
          carga.bytes = Some(length)
        }
      } finally QualityGates.registrar(gates.resultados, ejecucion.runId, Proceso, "bronze", settings.calidadDir)
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
        processBatch(batch, hadoopConf, settings)
      }
      .option("checkpointLocation", s"${settings.checkpointRoot}/l1-raw")
      .start()
}
