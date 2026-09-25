package raillytics.ingesta.config

// Una entrada de config/data_sources.yml.
case class DataSource(id: String, name: String, url: String, format: String)
