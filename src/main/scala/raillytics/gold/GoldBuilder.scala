package raillytics.gold

import org.apache.spark.sql.SparkSession
import raillytics.common.lake.LakeSettings
import raillytics.common.logging.Logging
import raillytics.common.trazabilidad.Cargas

import scala.io.Source

// Construye la capa Gold (modelo dimensional) con Spark, en batch: registra las
// tablas Silver como vistas, ejecuta el SQL de cada tabla del modelo
// (src/main/resources/gold/<tabla>.sql, dialecto Spark SQL) y escribe el
// resultado como Parquet en el bucket Gold, una tabla por prefijo.
//
// Es el paso Silver -> Gold del diseño (allí con destino Snowflake), con Spark
// como motor (por coherencia con las apps L1/L2) y Parquet en MinIO como
// destino, que es lo que Superset consulta con DuckDB. Gold se reconstruye entera en cada
// ejecución (full refresh): las dimensiones son agregaciones sobre todo
// Silver, así que no tiene sentido como stream de micro-batches.
object GoldBuilder extends Logging {

  // Tablas Silver de entrada; el SQL del modelo las referencia como silver_<tabla>.
  // Su contrato de columnas lo fija python/raillytics/procesamiento/silver_sample.py.
  private val SilverTables: Seq[String] = Seq("viajeros_enriquecidos", "puntualidad_enriquecida")

  // Dimensiones antes que hechos: un fallo en una dimensión aborta antes de
  // escribir hechos que no podrían resolverse.
  val GoldTables: Seq[String] = Seq("dim_fecha", "dim_estacion", "dim_linea", "fact_viajeros", "fact_puntualidad")

  // Registra cada tabla Silver (el prefijo entero: un único fichero o varios
  // part-*.parquet) como vista temporal de la sesión, que es lo que el SQL usa.
  private def registerSilver(lake: LakeSettings)(implicit spark: SparkSession): Unit =
    SilverTables.foreach { table =>
      spark.read.parquet(lake.silverTable(table)).createOrReplaceTempView(s"silver_$table")
    }

  // El SQL de cada tabla va como recurso; ${umbral_puntualidad_min} se sustituye
  // antes de ejecutarlo (Spark SQL no tiene variables de sesión portables).
  private def modelSql(table: String, umbralPuntualidadMin: Int): String = {
    val resource = s"/gold/$table.sql"
    val stream = Option(getClass.getResourceAsStream(resource))
      .getOrElse(throw new IllegalArgumentException(s"no existe el recurso $resource"))
    val sql = try Source.fromInputStream(stream, "UTF-8").mkString finally stream.close()
    sql.replace("${umbral_puntualidad_min}", umbralPuntualidadMin.toString)
  }

  // Construye y escribe las tablas Gold; devuelve las filas de cada una. Cada
  // ejecución queda en la trazabilidad de cargas (una fila por tabla).
  def build(settings: GoldBuilderSettings)(implicit spark: SparkSession): Map[String, Long] = {
    val parametros = Map("umbral_puntualidad_min" -> settings.umbralPuntualidadMin, "tablas" -> GoldTables)
    Cargas.registrar("gold_build", "gold", settings.lake.cargasDir, parametros) { ejecucion =>
      registerSilver(settings.lake)
      GoldTables.map { table =>
        val dest = settings.lake.goldTable(table)
        val rows = ejecucion.tabla(table, origen = Some(settings.lake.silverRoot), destino = Some(dest)) { carga =>
          // cache + count antes de escribir: si Silver está vacío o el SQL falla se
          // sabe antes de tocar el destino (overwrite vacía el prefijo entero), y el
          // recuento para la trazabilidad no vuelve a ejecutar la consulta.
          val df = spark.sql(modelSql(table, settings.umbralPuntualidadMin)).cache()
          val n = df.count()
          // Un único fichero por tabla: son pequeñas y así el prefijo queda
          // simple para quien lo lea con <tabla>/*.parquet (Superset, notebooks).
          df.coalesce(1).write.mode("overwrite").parquet(dest)
          df.unpersist()
          carga.filas = Some(n)
          n
        }
        logger.info(s"$table: $rows filas -> $dest")
        table -> rows
      }.toMap
    }
  }
}
