package raillytics.silver

import scala.io.Source

// El SQL de cada tabla Silver va como recurso (src/main/resources/silver/<tabla>.sql), como el de Gold. Lee la vista
// `entrada` (la foto o el micro-batch de L2, con las columnas de la fuente y _source_file) y devuelve la tabla Silver.
object SilverSql {
  def cargar(tabla: String): String = {
    val recurso = s"/silver/$tabla.sql"
    val stream = Option(getClass.getResourceAsStream(recurso))
      .getOrElse(throw new IllegalArgumentException(s"no existe el recurso $recurso (la tabla Silver '$tabla' declara su SQL ahí)"))
    try Source.fromInputStream(stream, "UTF-8").mkString finally stream.close()
  }
}
