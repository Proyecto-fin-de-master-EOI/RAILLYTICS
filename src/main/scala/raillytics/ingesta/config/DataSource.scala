package raillytics.ingesta.config

// Una entrada de config/data_sources.yml.
final case class DataSource(
  id: String,      // identificador corto: nombre de la carpeta en el staging y en los prefijos de MinIO
  name: String,    // nombre descriptivo, solo informativo
  url: String,     // de dónde descarga el DAG de Airflow (lado Python); las apps Scala no la usan
  format: String   // csv | json: cómo lo lee L2 (ver SourceFormat)
)
