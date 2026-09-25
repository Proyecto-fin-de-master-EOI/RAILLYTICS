package raillytics.gold

import org.apache.spark.sql.SparkSession
import raillytics.common.lake.LakePaths
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

  private val SilverTables: Seq[String] = Seq("viajeros_enriquecidos", "puntualidad_enriquecida")

  // Dimensiones antes que hechos: un fallo en una dimensión aborta antes de
  // escribir hechos que no podrían resolverse.
  val GoldTables: Seq[String] = Seq("dim_fecha", "dim_estacion", "dim_linea", "fact_viajeros", "fact_puntualidad")

  // Un servicio cuenta como puntual si llega con este retraso o menos (minutos).
  private val UmbralPuntualidadMin = 5

  private def registerSilver(silverRoot: String)(implicit spark: SparkSession): Unit =
    SilverTables.foreach { table =>
      spark.read.parquet(LakePaths.silverTable(silverRoot, table)).createOrReplaceTempView(s"silver_$table")
    }

  // El SQL de cada tabla va como recurso; ${umbral_puntualidad_min} se sustituye
  // antes de ejecutarlo (Spark SQL no tiene variables de sesión portables).
  private def modelSql(table: String): String = {
    val resource = s"/gold/$table.sql"
    val stream = Option(getClass.getResourceAsStream(resource))
      .getOrElse(throw new IllegalArgumentException(s"no existe el recurso $resource"))
    val sql = try Source.fromInputStream(stream, "UTF-8").mkString finally stream.close()
    sql.replace("${umbral_puntualidad_min}", UmbralPuntualidadMin.toString)
  }

  // Construye y escribe las tablas Gold; devuelve las filas de cada una. Cada
  // ejecución queda en la trazabilidad de cargas (una fila por tabla).
  def build(settings: GoldBuilderSettings)(implicit spark: SparkSession): Map[String, Long] = {
    val parametros = Map("umbral_puntualidad_min" -> UmbralPuntualidadMin, "tablas" -> GoldTables)
    Cargas.registrar("gold_build", "gold", settings.cargasDir, parametros) { ejecucion =>
      registerSilver(settings.silverRoot)
      GoldTables.map { table =>
        val dest = LakePaths.goldTable(settings.goldRoot, table)
        val rows = ejecucion.tabla(table, origen = Some(settings.silverRoot), destino = Some(dest)) { carga =>
          val df = spark.sql(modelSql(table)).cache()
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
