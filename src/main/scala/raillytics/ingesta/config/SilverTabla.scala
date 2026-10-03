package raillytics.ingesta.config

// Una tabla Silver que `make 04_silver` construye a partir de una fuente (clave `silver` de
// config/data_sources.yml). El SQL vive en src/main/resources/silver/<tabla>.sql.
//   snapshot     cada descarga es una foto completa: la última sustituye a la tabla
//   incremental  el resultado de cada micro-batch se añade a la tabla
final case class SilverTabla(tabla: String, modo: String) {
  require(SilverTabla.NombreValido.pattern.matcher(tabla).matches(),
    s"nombre de tabla Silver inválido '$tabla' (minúsculas, dígitos y '_', empezando por letra)")
  require(SilverTabla.Modos.contains(modo),
    s"modo Silver no soportado '$modo' (soportados: ${SilverTabla.Modos.toSeq.sorted.mkString(", ")})")

  def esSnapshot: Boolean = modo == SilverTabla.Snapshot
}

object SilverTabla {
  val Snapshot = "snapshot"
  val Incremental = "incremental"
  val Modos: Set[String] = Set(Snapshot, Incremental)
  val NombreValido = "^[a-z][a-z0-9_]*$".r
}
