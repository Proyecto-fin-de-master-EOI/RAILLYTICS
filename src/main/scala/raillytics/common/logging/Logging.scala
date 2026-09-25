package raillytics.common.logging

import org.slf4j.{Logger, LoggerFactory}

// Mezclar en cualquier object/clase para tener un logger SLF4J con el nombre
// de la clase (sin el "$" que Scala añade a los objects).
trait Logging {
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))
}
