package raillytics.ingesta.config

import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor
import raillytics.common.logging.Logging
import raillytics.ingesta.formats.SourceFormat

import java.io.{FileInputStream, InputStream}
import scala.jdk.CollectionConverters._

object DataSourceConfig extends Logging {
  private val SupportedFormats: Set[String] = SourceFormat.Supported

  def load(path: String): Seq[DataSource] = {
    logger.info(s"cargando fuentes de datos desde '$path'")
    val is = new FileInputStream(path)
    val sources = try loadFromStream(is)
    finally is.close()
    logger.info(s"${sources.size} fuente(s) cargada(s): ${sources.map(_.id).mkString(", ")}")
    sources
  }

  // Separado de load() para poder probarlo con un YAML en memoria.
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
