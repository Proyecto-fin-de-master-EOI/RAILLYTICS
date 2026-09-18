package raillytics.ingesta

import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor

import java.io.{FileInputStream, InputStream}
import scala.jdk.CollectionConverters._

case class DataSource(id: String, name: String, url: String, format: String)

object DataSourceConfig {
  val SupportedFormats: Set[String] = Set("csv", "json")

  private val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  def load(path: String): Seq[DataSource] = {
    logger.info(s"cargando fuentes de datos desde '$path'")
    val is = new FileInputStream(path)
    val sources = try loadFromStream(is)
    finally is.close()
    logger.info(s"${sources.size} fuente(s) cargada(s): ${sources.map(_.id).mkString(", ")}")
    sources
  }

  def loadFromStream(is: InputStream): Seq[DataSource] = {
    // SafeConstructor: el YAML solo trae mapas/listas/escalares, no hace falta
    // (ni conviene) permitir tags !!  que instancien clases Java arbitrarias.
    val yaml = new Yaml(new SafeConstructor(new LoaderOptions()))
    val root = yaml.load(is).asInstanceOf[java.util.Map[String, Object]]
    val rawSources = root.get("sources").asInstanceOf[java.util.List[java.util.Map[String, Object]]]

    rawSources.asScala.toSeq.map { m =>
      val format = requiredField(m, "format")
      require(
        SupportedFormats.contains(format),
        s"Formato no soportado: '$format' (soportados: ${SupportedFormats.mkString(", ")})"
      )
      DataSource(
        id = requiredField(m, "id"),
        name = requiredField(m, "name"),
        url = requiredField(m, "url"),
        format = format
      )
    }
  }

  // .asInstanceOf[String] sobre un valor ausente (null) tendría éxito
  // silenciosamente en Scala y produciría un DataSource con campos null en
  // vez de fallar -- esto exige que el campo esté presente, igual que el
  // parser Python (sources.py) ya lanza KeyError sobre el mismo caso.
  private def requiredField(m: java.util.Map[String, Object], field: String): String = {
    val value = m.get(field)
    require(value != null, s"Campo requerido ausente en config/data_sources.yml: '$field'")
    value.asInstanceOf[String]
  }
}
