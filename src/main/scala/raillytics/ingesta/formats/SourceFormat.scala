package raillytics.ingesta.formats

import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.apache.spark.sql.streaming.DataStreamReader

// Formatos admitidos en config/data_sources.yml y cómo los lee L2. Para añadir un
// formato tabular basta con añadir su entrada en readerOptions: DataSourceConfig
// lo valida y ParquetConverter lo lee sin más cambios. La descarga Python valida
// contra su propia lista, que debe coincidir con esta:
// python/raillytics/ingesta/formats.py.
object SourceFormat {

  // Cómo trata L2 cada formato.
  sealed trait Kind
  case object Tabular extends Kind   // csv, json, xml: Spark lo lee directamente (una query con esquema fijo por fuente)
  case object Archive extends Kind   // zip: L2 abre el archivo y lee cada miembro como csv (un Parquet por miembro)

  // formato tabular -> opciones de lectura de Spark (valen para batch y streaming).
  private val readerOptions: Map[String, Map[String, String]] = Map(
    "csv" -> Map("header" -> "true"),
    // Los feeds GTFS-RT (y JSON en general) suelen venir como un único objeto
    // JSON multi-línea, no JSON-Lines — sin multiLine, Spark trocea el fichero
    // línea a línea y casi todas las líneas acaban en _corrupt_record.
    "json" -> Map("multiLine" -> "true"),
    // El lector XML viene integrado en spark-sql desde Spark 4. No lleva opciones fijas: lo que
    // necesita —qué elemento es una fila— depende del documento, así que lo declara cada fuente
    // en `options.rowTag` (ver OptionsByFormat).
    "xml" -> Map.empty[String, String]
  )

  // Opciones que admite cada formato en la clave `options` de data_sources.yml. Cada formato admite
  // las suyas y solo las suyas: un `delimiter` en una fuente xml, o un `rowTag` en un csv, no
  // significan nada. Debe coincidir con OPCIONES_POR_FORMATO de formats.py.
  val OptionsByFormat: Map[String, Set[String]] = Map(
    "csv" -> Set("delimiter", "encoding"),
    // `rowTag` es obligatorio en xml: sin él el lector no sabe qué elemento es una fila.
    "xml" -> Set("rowTag", "encoding")
  )

  // Archivo ZIP cuyos miembros son CSV (p. ej. un GTFS estático: stops.txt, routes.txt...).
  val Zip = "zip"
  // Los miembros de un zip se leen con las opciones de este formato.
  val ArchiveMemberFormat = "csv"

  // Formatos válidos en el YAML.
  val Supported: Set[String] = readerOptions.keySet + Zip

  def kind(format: String): Kind = if (format == Zip) Archive else Tabular

  def options(format: String): Map[String, String] = options(format, Map.empty)

  // Opciones de una fuente concreta: las de su formato más las que declara en data_sources.yml
  // (`options`), que se llaman igual que las del lector de Spark: delimiter y encoding en csv,
  // rowTag en xml.
  def options(format: String, extra: Map[String, String]): Map[String, String] =
    readerOptions.getOrElse(format, Map.empty) ++ extra

  // Lector de streaming ya configurado para el formato; la ruta la pone quien
  // llama (.load), porque depende de la fuente.
  def streamReader(format: String, extra: Map[String, String] = Map.empty)(implicit spark: SparkSession): DataStreamReader =
    spark.readStream.format(format).options(options(format, extra))

  // El mismo lector en batch: L2 lo usa para inferir el esquema de la query al arrancar.
  def batchReader(format: String, extra: Map[String, String] = Map.empty)(implicit spark: SparkSession): DataFrameReader =
    spark.read.format(format).options(options(format, extra))
}
