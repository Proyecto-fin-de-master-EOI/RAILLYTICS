package raillytics.ingesta.l2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, input_file_name}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{StringType, StructType}
import raillytics.common.calidad.QualityGates
import raillytics.common.fs.HadoopFs
import raillytics.common.lake.BronzePaths
import raillytics.common.logging.Logging
import raillytics.common.trazabilidad.Cargas
import raillytics.ingesta.config.DataSource
import raillytics.ingesta.formats.SourceFormat

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path => NioPath}
import java.time.LocalDate
import java.util.zip.ZipInputStream
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

// L2: una query por fuente que lee lo que L1 dejó en l1DoneRoot, lo escribe
// como Parquet en MinIO (l2) y mueve los ficheros a processedRoot.
//
// Quality gates por micro-batch (raillytics.common.calidad.QualityGates): un
// fichero que no los pasa va a cuarentena (rejectedRoot/<fuente>/, con un
// <fichero>.rechazo.txt que explica por qué) y el resto del batch se convierte.
//   cabecera_csv          la cabecera de cada csv coincide con el esquema de la query
//   registros_corruptos   ningún registro en _corrupt_record (modo PERMISSIVE de Spark)
//   zip_valido            el zip se abre y trae al menos un miembro csv
//   filas_convertidas     (aviso) el batch ha escrito algún registro
//
// Idempotencia: cada micro-batch deja un marcador en el checkpoint al terminar de
// escribir; si Spark vuelve a entregar el mismo batch tras un reinicio (el
// checkpoint no llegó a confirmarse), solo se terminan de mover los ficheros, sin
// volver a escribir Parquet (evita duplicados en l2).
object ParquetConverter extends Logging {

  // Columna de bookkeeping (no analítica): readStream sobre csv/json no expone
  // "path" como sí hace binaryFile, así que hace falta input_file_name() para
  // saber qué ficheros componen cada micro-batch y poder moverlos después.
  val SourceFileCol = "_source_file"
  // Donde Spark deja los registros que no pudo parsear (modo PERMISSIVE).
  val CorruptRecordCol = "_corrupt_record"

  private val Proceso = "bronze_l2_parquet_converter"
  private val MarkerDir = "raillytics-batches"
  private val SidecarSuffix = ".rechazo.txt"

  def checkpointDir(settings: ParquetConverterSettings, source: DataSource): String =
    s"${settings.checkpointRoot}/l2/${source.id}"

  // Marcador de "batch ya escrito": dentro del checkpoint de la query para que
  // comparta su destino (borrar el checkpoint = reprocesar todo, también esto).
  def batchMarker(settings: ParquetConverterSettings, source: DataSource, batchId: Long): Path =
    new Path(s"${checkpointDir(settings, source)}/$MarkerDir/$batchId")

  // ------------------------------------------------------------------ esquema

  // Esquema fijo de la query, inferido UNA vez al arrancar con el lector batch
  // (csv: todas las columnas string con los nombres de la cabecera; json: lo que
  // Spark deduzca) más _corrupt_record, que Spark solo rellena si está en el esquema.
  def inferSchema(source: DataSource, inputGlob: String)(implicit spark: SparkSession): StructType =
    withCorruptRecordCol(SourceFormat.batchReader(source.format).load(inputGlob).schema)

  private def withCorruptRecordCol(schema: StructType): StructType =
    if (schema.fieldNames.contains(CorruptRecordCol)) schema else schema.add(CorruptRecordCol, StringType)

  // ------------------------------------------------------------------ query

  // Una query por fuente, con su propio checkpoint: cada una avanza (y falla) de
  // forma independiente, y el esquema se infiere por fuente, no mezclando formatos.
  def startQuery(source: DataSource, settings: ParquetConverterSettings, hadoopConf: Configuration)
                (implicit spark: SparkSession): StreamingQuery = {
    logger.debug(s"preparando query de la fuente '${source.id}' (formato=${source.format})")
    val input = s"${settings.l1DoneRoot}/${source.id}/*"   // plano, sin recursión (hermano de data/bronze/)
    val stream = SourceFormat.kind(source.format) match {
      case SourceFormat.Tabular =>
        SourceFormat.streamReader(source.format)
          .schema(inferSchema(source, input))
          .option("columnNameOfCorruptRecord", CorruptRecordCol)
          .load(input)
          .withColumn(SourceFileCol, input_file_name())
      case SourceFormat.Archive =>
        // binaryFile: path, modificationTime, length, content. El zip se abre con
        // Hadoop desde el path; la columna content no se materializa.
        spark.readStream.format("binaryFile").load(input)
    }
    stream.writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        processBatch(source, batch, batchId, hadoopConf, settings)
      }
      .option("checkpointLocation", checkpointDir(settings, source))
      .start()
  }

  // Si el directorio L1 de una fuente está vacío (su estado normal en reposo,
  // si L2 arranca antes que L1, o una fuente recién registrada), Spark no
  // puede inferir el esquema y la query no puede arrancar.
  private def isWaitingForFiles(e: Throwable): Boolean = e match {
    case ae: AnalysisException => Set("UNABLE_TO_INFER_SCHEMA", "PATH_NOT_FOUND").contains(ae.getCondition)
    case _                     => false
  }

  // Cada fuente arranca su query de forma independiente: sin el Try, el fallo
  // de una tumbaría main() entero y ninguna otra fuente (aunque esté sana)
  // llegaría a arrancar. Devuelve las fuentes que siguen sin ficheros en L1,
  // para reintentarlas más tarde; las que fallan por otro motivo se descartan.
  def startAll(sources: Seq[DataSource], settings: ParquetConverterSettings, hadoopConf: Configuration)
              (implicit spark: SparkSession): Seq[DataSource] =
    sources.filter { source =>
      Try(startQuery(source, settings, hadoopConf)) match {
        case Success(_) =>
          logger.info(s"query iniciada para la fuente '${source.id}'")
          false
        case Failure(e) if isWaitingForFiles(e) =>
          logger.debug(s"la fuente '${source.id}' aún no tiene ficheros en L1")
          true
        case Failure(e) =>
          logger.warn(s"no se pudo iniciar la query para '${source.id}': ${e.getMessage}", e)
          false
      }
    }

  // ------------------------------------------------------------------ micro-batch

  // Procesa un micro-batch de una fuente: escribe su Parquet en l2/<fuente>/<fecha>/
  // y mueve los ficheros de origen a processedRoot (o a rejectedRoot si no pasan
  // los gates). Si la escritura falla no se mueve nada y el checkpoint no avanza,
  // así que el siguiente arranque reintenta el mismo batch.
  def processBatch(source: DataSource, batch: DataFrame, batchId: Long, hadoopConf: Configuration,
                   settings: ParquetConverterSettings): Unit = {
    implicit val spark: SparkSession = batch.sparkSession
    val localFs = HadoopFs.local(hadoopConf)
    val marker = batchMarker(settings, source, batchId)
    val markerFs = marker.getFileSystem(hadoopConf)
    if (markerFs.exists(marker)) reanudarBatch(source, batchId, marker, markerFs, localFs, settings)
    else SourceFormat.kind(source.format) match {
      case SourceFormat.Tabular => processTabularBatch(source, batch, marker, markerFs, localFs, settings)
      case SourceFormat.Archive => processArchiveBatch(source, batch, marker, markerFs, localFs, settings)
    }
  }

  private def processTabularBatch(source: DataSource, batch: DataFrame, marker: Path, markerFs: FileSystem,
                                  localFs: FileSystem, settings: ParquetConverterSettings)
                                 (implicit spark: SparkSession): Unit = {
    // El batch se evalúa varias veces (ficheros, gates, escritura, recuento):
    // cacheado se leen y parsean los ficheros una sola vez.
    batch.persist()
    try {
      val files = batch.select(SourceFileCol).distinct().collect().map(_.getString(0)).toSeq.sorted
      logger.info(s"micro-batch de la fuente '${source.id}': ${files.size} fichero(s)")
      // Micro-batch vacío: nada que escribir ni que registrar (el return no se
      // salta el finally, el unpersist se hace igual).
      if (files.isEmpty) return

      val destPath = BronzePaths.l2(settings.bronzeRoot, source.id, LocalDate.now())
      val columnas = batch.columns.filterNot(c => c == SourceFileCol || c == CorruptRecordCol).toSeq
      val parametros = Map("fuente" -> source.id, "formato" -> source.format, "ficheros" -> files.size)
      // Trazabilidad: una fila por micro-batch y fuente (filas = registros escritos en
      // Parquet), una por fichero en cuarentena (estado error) y los gates del batch.
      Cargas.registrar(Proceso, "bronze", settings.cargasDir, parametros) { ejecucion =>
        val gates = new QualityGates.Acumulador
        val rechazados = mutable.LinkedHashMap.empty[String, String]
        try {
          // Gate 1 (csv): la cabecera de cada fichero es la del esquema de la query.
          // Spark casa las columnas por posición e ignora los nombres de la cabecera
          // (enforceSchema): un fichero con otro orden o conjunto de columnas se
          // convertiría con los datos cambiados de columna sin que nada avisara.
          if (source.format == "csv") {
            val distintas = files.flatMap { f =>
              val cabecera = cabeceraCsv(localFs, f)
              if (cabeceraCoincide(cabecera, columnas)) None else Some(f -> cabecera)
            }
            distintas.foreach { case (f, h) =>
              rechazados(f) = s"cabecera [${h.mkString(",")}] distinta del esquema de la query [${columnas.mkString(",")}]"
            }
            gates += QualityGates.resultado(source.id, "cabecera_csv", QualityGates.Bloqueante, distintas.isEmpty,
              distintas.size.toDouble, "= 0", Some(s"${distintas.size} fichero(s) con otra cabecera: ${distintas.map(d => nombre(d._1)).mkString(", ")}"))
          }
          // Gate 2: registros que Spark no pudo parsear (fila entera en _corrupt_record).
          if (batch.columns.contains(CorruptRecordCol)) {
            val corruptos = batch.filter(col(CorruptRecordCol).isNotNull).groupBy(SourceFileCol).count()
              .collect().map(r => r.getString(0) -> r.getLong(1)).toMap
            corruptos.foreach { case (f, n) => if (!rechazados.contains(f)) rechazados(f) = s"$n registro(s) corrupto(s)" }
            gates += QualityGates.resultado(source.id, "registros_corruptos", QualityGates.Bloqueante, corruptos.isEmpty,
              corruptos.values.sum.toDouble, "= 0",
              Some(corruptos.toSeq.sortBy(_._1).map { case (f, n) => s"${nombre(f)} ($n)" }.mkString(", ")))
          }

          val aceptados = files.filterNot(rechazados.contains)
          val buenos = if (rechazados.isEmpty) batch else batch.filter(!col(SourceFileCol).isin(rechazados.keys.toSeq: _*))
          val datos = buenos.drop(SourceFileCol, CorruptRecordCol)
          val n = datos.count()
          gates += QualityGates.resultado(source.id, "filas_convertidas", QualityGates.Aviso, n > 0, n.toDouble, "> 0",
            Some("el batch no ha producido ningún registro"), tipo = "batch")

          val origen = files.headOption.map(f => new Path(f).getParent.toString)
          ejecucion.tabla(source.id, origen = origen, destino = Some(destPath)) { carga =>
            if (n > 0) datos.write.mode("append").parquet(destPath)
            logger.debug(s"batch de '${source.id}' escrito en '$destPath' ($n filas)")
            finalizarBatch(source, ejecucion, aceptados, rechazados.toSeq, marker, markerFs, localFs, settings)
            carga.filas = Some(n)
          }
        } finally QualityGates.registrar(gates.resultados, ejecucion.runId, Proceso, "bronze", settings.calidadDir)
      }
    } finally batch.unpersist()
  }

  // Fuente zip: cada archivo se abre con Hadoop, se extrae a un directorio
  // temporal y cada miembro (.txt/.csv) se lee como csv y se escribe en su propio
  // prefijo l2/<fuente>/<fecha>/<miembro>/. Todo o nada por archivo: si un
  // miembro no se puede leer, el zip entero va a cuarentena y no se escribe nada.
  private def processArchiveBatch(source: DataSource, batch: DataFrame, marker: Path, markerFs: FileSystem,
                                  localFs: FileSystem, settings: ParquetConverterSettings)
                                 (implicit spark: SparkSession): Unit = {
    val files = batch.select("path").collect().map(_.getString(0)).toSeq.sorted
    logger.info(s"micro-batch de la fuente '${source.id}' (zip): ${files.size} fichero(s)")
    if (files.isEmpty) return

    val date = LocalDate.now()
    val parametros = Map("fuente" -> source.id, "formato" -> source.format, "ficheros" -> files.size)
    Cargas.registrar(Proceso, "bronze", settings.cargasDir, parametros) { ejecucion =>
      val gates = new QualityGates.Acumulador
      val aceptados = mutable.ListBuffer.empty[String]
      val rechazados = mutable.LinkedHashMap.empty[String, String]
      try {
        files.foreach { uri =>
          val tmp = Files.createTempDirectory("raillytics-l2-zip")
          try convertirZip(source, uri, tmp, date, localFs, ejecucion, gates, settings) match {
            case None         => aceptados += uri
            case Some(motivo) => rechazados(uri) = motivo
          } finally borrarDirectorio(tmp)
        }
        finalizarBatch(source, ejecucion, aceptados.toSeq, rechazados.toSeq, marker, markerFs, localFs, settings)
      } finally QualityGates.registrar(gates.resultados, ejecucion.runId, Proceso, "bronze", settings.calidadDir)
    }
  }

  // Convierte un zip; None si se ha escrito, Some(motivo) si va a cuarentena.
  private def convertirZip(source: DataSource, uri: String, tmp: NioPath, date: LocalDate, localFs: FileSystem,
                           ejecucion: Cargas.Ejecucion, gates: QualityGates.Acumulador, settings: ParquetConverterSettings)
                          (implicit spark: SparkSession): Option[String] = {
    val fileName = nombre(uri)
    extraerZip(localFs, uri, tmp) match {
      case Failure(e) =>
        gates += QualityGates.resultado(source.id, "zip_valido", QualityGates.Bloqueante, pasa = false, 0, ">= 1 miembro csv",
          Some(s"$fileName: ${e.getMessage}"))
        Some(s"ZIP inválido: ${e.getMessage}")
      case Success(miembros) if miembros.isEmpty =>
        gates += QualityGates.resultado(source.id, "zip_valido", QualityGates.Bloqueante, pasa = false, 0, ">= 1 miembro csv",
          Some(s"$fileName: sin miembros .txt/.csv"))
        Some("el ZIP no contiene ningún miembro .txt/.csv")
      case Success(miembros) =>
        gates += QualityGates.resultado(source.id, "zip_valido", QualityGates.Bloqueante, pasa = true, miembros.size.toDouble, ">= 1 miembro csv")
        val leidos = miembros.map { case (member, path) => member -> leerMiembro(path) }
        val ilegibles = leidos.collect { case (m, Failure(e)) => s"$m (${primeraLinea(e.getMessage)})" }
        val dfs = leidos.collect { case (m, Success(df)) => m -> df.persist() }
        try {
          dfs.foreach(_._2.count())   // materializa la caché con todas las columnas antes de filtrar por _corrupt_record
          val corruptos = dfs.map { case (m, df) => m -> df.filter(col(CorruptRecordCol).isNotNull).count() }.filter(_._2 > 0)
          val problemas = ilegibles ++ corruptos.map { case (m, n) => s"$m ($n registro(s) corrupto(s))" }
          gates += QualityGates.resultado(source.id, "registros_corruptos", QualityGates.Bloqueante, problemas.isEmpty,
            (corruptos.map(_._2).sum + ilegibles.size).toDouble, "= 0", Some(s"$fileName: ${problemas.mkString(", ")}"))
          if (problemas.nonEmpty) Some(s"miembros ilegibles o con registros corruptos: ${problemas.mkString(", ")}")
          else {
            dfs.foreach { case (member, df) =>
              val dest = BronzePaths.l2Member(settings.bronzeRoot, source.id, date, member)
              ejecucion.tabla(s"${source.id}/$member", origen = Some(uri), destino = Some(dest)) { carga =>
                val datos = df.drop(CorruptRecordCol)
                val n = datos.count()
                if (n > 0) datos.write.mode("append").parquet(dest)
                carga.filas = Some(n)
              }
            }
            None
          }
        } finally dfs.foreach(_._2.unpersist())
    }
  }

  // Extrae los miembros .txt/.csv del zip (sin rutas: solo el nombre base, sin
  // extensión y saneado, que será el nombre del prefijo Parquet).
  private def extraerZip(fs: FileSystem, uri: String, tmp: NioPath): Try[Seq[(String, NioPath)]] = Try {
    val miembros = mutable.LinkedHashMap.empty[String, NioPath]
    val zin = new ZipInputStream(fs.open(new Path(uri)))
    try {
      Iterator.continually(zin.getNextEntry).takeWhile(_ != null).foreach { entry =>
        val base = entry.getName.split("[/\\\\]").last
        val extension = base.lastIndexOf('.') match { case -1 => "" case i => base.substring(i + 1).toLowerCase }
        if (!entry.isDirectory && Set("txt", "csv").contains(extension) && !base.startsWith(".")) {
          val member = base.substring(0, base.length - extension.length - 1).replaceAll("[^A-Za-z0-9_-]", "_")
          require(!miembros.contains(member), s"dos miembros con el mismo nombre '$member'")
          val dest = tmp.resolve(s"$member.csv")
          Files.copy(zin, dest)
          miembros(member) = dest
        }
        zin.closeEntry()
      }
    } finally zin.close()
    miembros.toSeq
  }

  // Un miembro extraído, leído como csv con esquema explícito (todo string) y _corrupt_record.
  private def leerMiembro(path: NioPath)(implicit spark: SparkSession): Try[DataFrame] = Try {
    val uri = path.toUri.toString
    val opciones = SourceFormat.options(SourceFormat.ArchiveMemberFormat)
    val base = spark.read.format(SourceFormat.ArchiveMemberFormat).options(opciones).load(uri).schema
    spark.read.format(SourceFormat.ArchiveMemberFormat).options(opciones)
      .schema(withCorruptRecordCol(base))
      .option("columnNameOfCorruptRecord", CorruptRecordCol)
      .load(uri)
  }

  // ------------------------------------------------------------------ cierre del batch

  // Marcador + movimientos. El marcador va ANTES de mover: si el proceso muere a
  // medias, el reintento del batch encuentra el marcador y solo termina de mover.
  private def finalizarBatch(source: DataSource, ejecucion: Cargas.Ejecucion, aceptados: Seq[String],
                             rechazados: Seq[(String, String)], marker: Path, markerFs: FileSystem, localFs: FileSystem,
                             settings: ParquetConverterSettings): Unit = {
    escribirMarker(markerFs, marker, aceptados, rechazados)
    moveProcessedFiles(aceptados, localFs, source.id, settings.processedRoot)
    rechazados.foreach { case (uri, motivo) =>
      val dest = moverACuarentena(localFs, uri, source.id, settings.rejectedRoot, motivo)
      // Fila de trazabilidad en estado error, sin abortar el batch.
      ejecucion.tabla(source.id, origen = Some(uri), destino = Some(dest.toString)) { carga =>
        carga.estado = Cargas.EstadoError
        carga.error = Some(s"cuarentena: $motivo")
      }
      logger.warn(s"fichero '${nombre(uri)}' de la fuente '${source.id}' en cuarentena ($dest): $motivo")
    }
  }

  // Batch repetido tras un reinicio: el Parquet ya está escrito; solo quedan por
  // mover los ficheros que sigan en l1DoneRoot.
  private def reanudarBatch(source: DataSource, batchId: Long, marker: Path, markerFs: FileSystem, localFs: FileSystem,
                            settings: ParquetConverterSettings): Unit = {
    val lineas = leerLineas(markerFs, marker).map(_.split("\t", 3))
    val ok = lineas.collect { case Array("ok", uri) => uri }.filter(u => localFs.exists(new Path(u)))
    val rech = lineas.collect { case Array("rechazado", uri, motivo) => (uri, motivo) }.filter(r => localFs.exists(new Path(r._1)))
    logger.info(s"batch $batchId de '${source.id}' ya escrito en una ejecución anterior: " +
      s"${ok.size + rech.size} fichero(s) pendiente(s) de mover, no se vuelve a escribir Parquet")
    moveProcessedFiles(ok, localFs, source.id, settings.processedRoot)
    rech.foreach { case (uri, motivo) => moverACuarentena(localFs, uri, source.id, settings.rejectedRoot, motivo) }
  }

  // Mueve los ficheros ya convertidos de l1DoneRoot a processedRoot/<fuente>: así
  // no vuelven a entrar en ningún glob y queda claro qué completó L1 y L2.
  def moveProcessedFiles(filePaths: Seq[String], localFs: FileSystem, source: String, processedRoot: String): Unit = {
    val destDir = new Path(s"$processedRoot/$source")
    filePaths.foreach { localUri =>
      val srcPath = new Path(localUri)
      logger.debug(s"moviendo '$srcPath' -> '$destDir' (fuente=$source)")
      HadoopFs.moveInto(localFs, srcPath, destDir)
    }
  }

  private def moverACuarentena(localFs: FileSystem, uri: String, source: String, rejectedRoot: String, motivo: String): Path = {
    val destDir = new Path(s"$rejectedRoot/$source")
    val dest = HadoopFs.moveInto(localFs, new Path(uri), destDir)
    val sidecar = localFs.create(new Path(destDir, dest.getName + SidecarSuffix), true)
    try sidecar.write((motivo + "\n").getBytes(UTF_8)) finally sidecar.close()
    dest
  }

  private def escribirMarker(fs: FileSystem, marker: Path, aceptados: Seq[String], rechazados: Seq[(String, String)]): Unit = {
    fs.mkdirs(marker.getParent)
    val lineas = aceptados.map(u => s"ok\t$u") ++ rechazados.map { case (u, m) => s"rechazado\t$u\t${m.replaceAll("[\\t\\r\\n]", " ")}" }
    val out = fs.create(marker, true)
    try out.write((lineas.mkString("\n") + "\n").getBytes(UTF_8)) finally out.close()
  }

  private def leerLineas(fs: FileSystem, path: Path): Seq[String] = {
    val in = fs.open(path)
    try {
      val reader = new BufferedReader(new InputStreamReader(in, UTF_8))
      Iterator.continually(reader.readLine()).takeWhile(_ != null).filter(_.nonEmpty).toList
    } finally in.close()
  }

  // ------------------------------------------------------------------ cabeceras csv

  // Primera línea del fichero, sin BOM ni comillas (como la vería Spark al inferir).
  private def cabeceraCsv(fs: FileSystem, uri: String): Seq[String] = {
    val in = fs.open(new Path(uri))
    val linea = try {
      val reader = new BufferedReader(new InputStreamReader(in, UTF_8))
      Option(reader.readLine()).getOrElse("")
    } finally in.close()
    linea.stripPrefix("﻿").split(",", -1).toSeq.map(_.trim.stripPrefix("\"").stripSuffix("\""))
  }

  // Coincide si tiene las mismas columnas en el mismo orden (sin distinguir
  // mayúsculas, como resuelve Spark). Una cabecera con nombres vacíos o repetidos
  // no se compara (Spark los renombra al inferir y no habría forma de casarlos).
  private def cabeceraCoincide(cabecera: Seq[String], esperada: Seq[String]): Boolean =
    cabecera.exists(_.isEmpty) || cabecera.distinct.size != cabecera.size ||
      cabecera.map(_.toLowerCase) == esperada.map(_.trim.toLowerCase)

  private def nombre(uri: String): String = new Path(uri).getName

  private def primeraLinea(s: String): String = Option(s).getOrElse("").linesIterator.toSeq.headOption.getOrElse("")

  private def borrarDirectorio(dir: NioPath): Unit =
    Try {
      val stream = Files.walk(dir)
      try stream.sorted(java.util.Comparator.reverseOrder[NioPath]()).forEach(p => Files.deleteIfExists(p))
      finally stream.close()
    }
}
