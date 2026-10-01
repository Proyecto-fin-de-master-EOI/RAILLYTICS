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
  case object Tabular extends Kind   // csv, json: Spark lo lee directamente (una query con esquema fijo por fuente)
  case object Archive extends Kind   // zip: L2 abre el archivo y lee cada miembro como csv (un Parquet por miembro)

  // formato tabular -> opciones de lectura de Spark (valen para batch y streaming).
  private val readerOptions: Map[String, Map[String, String]] = Map(
    "csv" -> Map("header" -> "true"),
    // Los feeds GTFS-RT (y JSON en general) suelen venir como un único objeto
    // JSON multi-línea, no JSON-Lines — sin multiLine, Spark trocea el fichero
    // línea a línea y casi todas las líneas acaban en _corrupt_record.
    "json" -> Map("multiLine" -> "true")
  )

  // Archivo ZIP cuyos miembros son CSV (p. ej. un GTFS estático: stops.txt, routes.txt...).
  val Zip = "zip"
  // Los miembros de un zip se leen con las opciones de este formato.
  val ArchiveMemberFormat = "csv"

  // Formatos válidos en el YAML.
  val Supported: Set[String] = readerOptions.keySet + Zip

  def kind(format: String): Kind = if (format == Zip) Archive else Tabular

  def options(format: String): Map[String, String] = readerOptions.getOrElse(format, Map.empty)

  // Lector de streaming ya configurado para el formato; la ruta la pone quien
  // llama (.load), porque depende de la fuente.
  def streamReader(format: String)(implicit spark: SparkSession): DataStreamReader =
    spark.readStream.format(format).options(options(format))

  // El mismo lector en batch: L2 lo usa para inferir el esquema de la query al arrancar.
  def batchReader(format: String)(implicit spark: SparkSession): DataFrameReader =
    spark.read.format(format).options(options(format))
}
