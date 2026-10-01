package raillytics.ingesta.config

import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor
import raillytics.common.logging.Logging
import raillytics.ingesta.formats.SourceFormat

import java.io.{FileInputStream, InputStream}
import scala.jdk.CollectionConverters._

// Lee el registro de fuentes config/data_sources.yml, compartido con la
// descarga Python (python/raillytics/ingesta/sources.py): los dos deben
// aceptar exactamente las mismas entradas.
object DataSourceConfig extends Logging {
  private val SupportedFormats: Set[String] = SourceFormat.Supported

  // Carga el YAML desde disco (en L2, la ruta de raillytics.ingesta.data-sources).
  def load(path: String): Seq[DataSource] = {
    logger.info(s"cargando fuentes de datos desde '$path'")
    val is = new FileInputStream(path)
    val sources = try loadFromStream(is)
    finally is.close()
    logger.info(s"${sources.size} fuente(s) cargada(s): ${sources.map(_.id).mkString(", ")}")
    sources
  }

  // Mismas listas que python/raillytics/ingesta/formats.py: una clave desconocida es un error en los dos parsers.
  private val ClavesFuente = Set("id", "name", "url", "format", "options", "checks", "silver")
  private val OpcionesLectura = Set("delimiter", "encoding")
  private val ChecksDescarga = Set("min_bytes", "min_filas", "columnas")

  // Separado de load() para poder probarlo con un YAML en memoria.
  def loadFromStream(is: InputStream): Seq[DataSource] = {
    // SafeConstructor: el YAML solo trae mapas/listas/escalares, no hace falta
    // (ni conviene) permitir tags !!  que instancien clases Java arbitrarias.
    val yaml = new Yaml(new SafeConstructor(new LoaderOptions()))
    val root = yaml.load(is).asInstanceOf[java.util.Map[String, Object]]
    val rawSources = root.get("sources").asInstanceOf[java.util.List[java.util.Map[String, Object]]]

    val sources = rawSources.asScala.toSeq.map(parsear)
    val tablas = sources.flatMap(_.silver.map(_.tabla))
    val repetidas = tablas.groupBy(identity).collect { case (t, v) if v.size > 1 => t }.toSeq.sorted
    require(repetidas.isEmpty, s"Tablas Silver declaradas por más de una fuente: ${repetidas.mkString(", ")}")
    sources
  }

  private def parsear(m: java.util.Map[String, Object]): DataSource = {
    // Primero lo que falta (un typo en `id` se ve como «falta 'id'»), después lo que sobra.
    val id = requiredField(m, "id")
    Seq("name", "url", "format").foreach(requiredField(m, _))
    val desconocidas = m.keySet.asScala.toSet -- ClavesFuente
    require(desconocidas.isEmpty,
      s"Clave desconocida en la fuente '$id': ${desconocidas.toSeq.sorted.mkString(", ")} (válidas: ${ClavesFuente.toSeq.sorted.mkString(", ")})")
    val format = requiredField(m, "format")
    require(
      SupportedFormats.contains(format),
      s"Formato no soportado: '$format' (soportados: ${SupportedFormats.mkString(", ")})"
    )
    val options = bloque(id, "options", m, OpcionesLectura).map { case (k, v) => k -> v.toString }
    val checks = bloque(id, "checks", m, ChecksDescarga)
    require(options.isEmpty && checks.isEmpty || format == "csv",
      s"Fuente '$id': 'options' y 'checks' solo se admiten en fuentes csv (formato '$format')")
    options.get("delimiter").foreach { d =>
      require(d.length == 1, s"Fuente '$id': options.delimiter debe ser un único carácter, no '$d'")
    }
    DataSource(
      id = requiredField(m, "id"),
      name = requiredField(m, "name"),
      url = requiredField(m, "url"),
      format = format,
      options = options,
      checks = checks,
      silver = silver(id, m)
    )
  }

  private def bloque(id: String, nombre: String, m: java.util.Map[String, Object], permitidas: Set[String]): Map[String, Any] =
    Option(m.get(nombre)) match {
      case None => Map.empty
      case Some(b: java.util.Map[_, _]) =>
        val mapa = b.asScala.map { case (k, v) => k.toString -> (v: Any) }.toMap
        val desconocidas = mapa.keySet -- permitidas
        require(desconocidas.isEmpty,
          s"Clave desconocida en $nombre de la fuente '$id': ${desconocidas.toSeq.sorted.mkString(", ")} (válidas: ${permitidas.toSeq.sorted.mkString(", ")})")
        mapa
      case Some(otro) => throw new IllegalArgumentException(s"Fuente '$id': '$nombre' debe ser un mapa clave: valor, no '$otro'")
    }

  private def silver(id: String, m: java.util.Map[String, Object]): Seq[SilverTabla] =
    Option(m.get("silver")) match {
      case None => Seq.empty
      case Some(l: java.util.List[_]) =>
        l.asScala.toSeq.map {
          case e: java.util.Map[_, _] =>
            val entrada = e.asScala.map { case (k, v) => k.toString -> v }.toMap
            require((entrada.keySet -- Set("tabla", "modo")).isEmpty, s"Fuente '$id': cada entrada de 'silver' es {tabla, modo}, no $entrada")
            Seq("tabla", "modo").foreach(k => require(entrada.contains(k), s"Fuente '$id': a una entrada de 'silver' le falta '$k'"))
            SilverTabla(entrada("tabla").toString, entrada("modo").toString)
          case otro => throw new IllegalArgumentException(s"Fuente '$id': cada entrada de 'silver' es {tabla, modo}, no '$otro'")
        }
      case Some(otro) => throw new IllegalArgumentException(s"Fuente '$id': 'silver' debe ser una lista, no '$otro'")
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
