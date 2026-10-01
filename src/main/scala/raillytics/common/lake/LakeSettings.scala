package raillytics.common.lake

import com.typesafe.config.Config

// Raíces de Bronze, Silver, Gold y trazabilidad: claves raillytics.lake.* de
// application.conf (por defecto los buckets de MinIO con esquema s3a://, y
// SILVER_ROOT / GOLD_ROOT / TRAZABILIDAD_ROOT para apuntar a directorios
// locales, como hacen los tests). Son las mismas variables que usa el lado
// Python (python/raillytics/utils/lake.py). Las rutas dentro de Bronze
// (l1-raw/, l2/) siguen en BronzePaths.
final case class LakeSettings(
  bronzeRoot: String,
  silverRoot: String,
  goldRoot: String,
  trazabilidadRoot: String
) {
  // Cada tabla vive en su propio prefijo: Spark lee y escribe el directorio
  // entero (part-*.parquet) y DuckDB/Superset lo leen con <tabla>/*.parquet.
  def silverTable(table: String): String = s"$silverRoot/$table/"

  def goldTable(table: String): String = s"$goldRoot/$table/"

  // Registro de cargas (raillytics.common.trazabilidad.Cargas); el mismo
  // directorio en el que escribe el lado Python.
  def cargasDir: String = s"$trazabilidadRoot/cargas/"

  // Resultados de quality gates (raillytics.common.calidad.QualityGates), hermano
  // de cargas/ y con el mismo run_id; también lo escribe el lado Python.
  def calidadDir: String = s"$trazabilidadRoot/calidad/"
}

object LakeSettings {
  def from(config: Config): LakeSettings = {
    val lake = config.getConfig("raillytics.lake")
    LakeSettings(
      bronzeRoot = lake.getString("bronze-root"),
      silverRoot = lake.getString("silver-root"),
      goldRoot = lake.getString("gold-root"),
      trazabilidadRoot = lake.getString("trazabilidad-root")
    )
  }
}
