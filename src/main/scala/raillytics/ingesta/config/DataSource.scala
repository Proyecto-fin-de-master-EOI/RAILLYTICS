package raillytics.ingesta.config

// Una entrada de config/data_sources.yml.
final case class DataSource(
  id: String,      // identificador corto: nombre de la carpeta en el staging y en los prefijos de MinIO
  name: String,    // nombre descriptivo, solo informativo
  url: String,     // de dónde descarga el DAG de Airflow (lado Python); las apps Scala no la usan
  format: String,  // csv | json | zip: cómo lo lee L2 (ver SourceFormat)
  // Cómo se lee un csv (delimiter, encoding): L2 las suma a las opciones del formato y la descarga Python las usa al validar.
  options: Map[String, String] = Map.empty,
  // Reglas de la descarga (min_bytes, min_filas, columnas): las aplica Python; aquí solo se valida que la clave exista.
  checks: Map[String, Any] = Map.empty,
  // Tablas Silver que `make 04_silver` construye a partir de esta fuente.
  silver: Seq[SilverTabla] = Seq.empty
)
