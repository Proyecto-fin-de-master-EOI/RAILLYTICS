package raillytics.common.lake

import java.time.LocalDate

// Rutas dentro del bucket Bronze. Los prefijos l1-raw/ y l2/ son parte del
// contrato del lake (los conocen Superset y el lado Python), no configuración
// por entorno: por eso viven aquí y no en application.conf. La fecha es la de
// la subida (LocalDate.now() en las apps), no la del dato.
object BronzePaths {
  // L1: el fichero tal cual llegó del staging, bajo l1-raw/<fuente>/<fecha>/.
  def l1(bronzeRoot: String, source: String, date: LocalDate, fileName: String): String =
    s"$bronzeRoot/l1-raw/$source/$date/$fileName"

  // L2: directorio (con barra final) donde Spark deja el Parquet de la fuente
  // para esa fecha; cada micro-batch añade sus part-*.parquet.
  def l2(bronzeRoot: String, source: String, date: LocalDate): String =
    s"$bronzeRoot/l2/$source/$date/"
}
