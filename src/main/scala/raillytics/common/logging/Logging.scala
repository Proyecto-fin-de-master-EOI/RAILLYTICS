package raillytics.common.logging

import org.slf4j.{Logger, LoggerFactory}

// Mezclar en cualquier object/clase para tener un logger SLF4J con el nombre
// de la clase (sin el "$" que Scala añade a los objects). Es lazy para que el
// logger se cree en el primer uso y no al inicializar el object, que en Scala
// puede ocurrir antes de que log4j2 haya leído su configuración.
trait Logging {
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))
}
