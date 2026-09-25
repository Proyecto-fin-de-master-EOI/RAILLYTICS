package raillytics.ingesta.config

// Una entrada de config/data_sources.yml.
final case class DataSource(id: String, name: String, url: String, format: String)
