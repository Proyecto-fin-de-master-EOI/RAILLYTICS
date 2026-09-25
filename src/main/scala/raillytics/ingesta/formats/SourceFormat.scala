package raillytics.ingesta.formats

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader

// Formatos admitidos en config/data_sources.yml y las opciones de lectura de
// cada uno. Para añadir un formato basta con añadir su entrada aquí:
// DataSourceConfig lo valida y ParquetConverter lo lee sin más cambios.
object SourceFormat {

  private val readerOptions: Map[String, DataStreamReader => DataStreamReader] = Map(
    "csv" -> (_.option("header", "true")),
    // Los feeds GTFS-RT (y JSON en general) suelen venir como un único objeto
    // JSON multi-línea, no JSON-Lines — sin multiLine, Spark trocea el fichero
    // línea a línea y casi todas las líneas acaban en _corrupt_record.
    "json" -> (_.option("multiLine", "true"))
  )

  val Supported: Set[String] = readerOptions.keySet

  def streamReader(format: String)(implicit spark: SparkSession): DataStreamReader = {
    val reader = spark.readStream.format(format)
    readerOptions.get(format).fold(reader)(configure => configure(reader))
  }
}
