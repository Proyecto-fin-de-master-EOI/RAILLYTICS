package raillytics.gold

import org.apache.spark.sql.{DataFrame, SparkSession}
import raillytics.common.calidad.QualityGates
import raillytics.common.lake.{LakeSettings, LakeViews}
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
//
// Quality gates (config/quality_gates.yml, raillytics.common.calidad.QualityGates):
//   1. entrada: Silver debe cumplir su contrato; si no, no se construye nada.
//   2. salida: todas las tablas Gold se construyen en memoria y se validan ANTES de
//      escribir; si falla un gate bloqueante, Gold se queda como estaba (ninguna
//      tabla se sobrescribe a medias).
object GoldBuilder extends Logging {

  private val Proceso = "gold_build"

  // Tablas Silver de entrada; el SQL del modelo las referencia como silver_<tabla>.
  // Su contrato de columnas lo fija python/raillytics/procesamiento/silver_sample.py.
  private val SilverTables: Seq[String] = Seq("viajeros_enriquecidos", "puntualidad_enriquecida")

  // Dimensiones antes que hechos: un fallo en una dimensión aborta antes de
  // escribir hechos que no podrían resolverse.
  val GoldTables: Seq[String] = Seq("dim_fecha", "dim_estacion", "dim_linea", "fact_viajeros", "fact_puntualidad")

  private def silverViews: Seq[String] = SilverTables.map(LakeViews.SilverPrefix + _)
  private def goldViews: Seq[String] = GoldTables.map(LakeViews.GoldPrefix + _)

  // Registra cada tabla Silver (el prefijo entero: un único fichero o varios
  // part-*.parquet) como vista temporal de la sesión, que es lo que el SQL usa.
  // Sin Silver no hay Gold: un prefijo ilegible es un error, no un aviso.
  private def registerSilver(lake: LakeSettings)(implicit spark: SparkSession): Unit =
    LakeViews.registrar(lake, silverViews).headOption.foreach { case (vista, e) =>
      throw new IllegalStateException(s"no se puede leer la tabla Silver '$vista' bajo ${lake.silverRoot}: ${e.getMessage}", e)
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

  // Construye, valida y escribe las tablas Gold; devuelve las filas de cada una.
  // Cada ejecución queda en la trazabilidad de cargas (una fila por tabla) y en
  // la de calidad (una fila por gate, mismo run_id).
  def build(settings: GoldBuilderSettings)(implicit spark: SparkSession): Map[String, Long] = {
    val parametros = Map("umbral_puntualidad_min" -> settings.umbralPuntualidadMin, "tablas" -> GoldTables,
                         "quality_gates" -> settings.calidadConfig)
    Cargas.registrar(Proceso, "gold", settings.lake.cargasDir, parametros) { ejecucion =>
      val gates = QualityGates.load(settings.calidadConfig)
      registerSilver(settings.lake)

      // 1. Gate de entrada: el contrato de Silver.
      val silverResultados = QualityGates.evaluarTablas(gates, silverViews)
      QualityGates.registrar(silverResultados, ejecucion.runId, Proceso, "silver", settings.lake.calidadDir)
      logger.info("entrada Silver: " + QualityGates.resumen(silverResultados).replace("\n", " | "))
      QualityGates.exigir(silverResultados)

      // 2. Todas las tablas en memoria, sin tocar el destino. cache + count: si el SQL
      //    falla se sabe aquí, y ni los gates ni el recuento vuelven a ejecutar la consulta.
      val construidas: Seq[(String, DataFrame, Long)] = GoldTables.map { table =>
        val df = spark.sql(modelSql(table, settings.umbralPuntualidadMin)).cache()
        val n = df.count()
        df.createOrReplaceTempView(LakeViews.GoldPrefix + table)   // para los gates gold_* (y su conciliación con silver_*)
        (table, df, n)
      }
      try {
        // 3. Gate de salida: sobre lo construido, antes de sobrescribir nada.
        val goldResultados = QualityGates.evaluarTablas(gates, goldViews)
        QualityGates.registrar(goldResultados, ejecucion.runId, Proceso, "gold", settings.lake.calidadDir)
        logger.info("salida Gold: " + QualityGates.resumen(goldResultados).replace("\n", " | "))
        QualityGates.exigir(goldResultados)

        // 4. Escritura. Un único fichero por tabla: son pequeñas y así el prefijo queda
        //    simple para quien lo lea con <tabla>/*.parquet (Superset, notebooks).
        construidas.map { case (table, df, n) =>
          val dest = settings.lake.goldTable(table)
          ejecucion.tabla(table, origen = Some(settings.lake.silverRoot), destino = Some(dest)) { carga =>
            df.coalesce(1).write.mode("overwrite").parquet(dest)
            carga.filas = Some(n)
          }
          logger.info(s"$table: $n filas -> $dest")
          table -> n
        }.toMap
      } finally construidas.foreach(_._2.unpersist())
    }
  }
}
