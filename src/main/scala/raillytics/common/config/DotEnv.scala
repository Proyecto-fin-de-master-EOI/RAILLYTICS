package raillytics.common.config

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

// Lectura mínima del .env del proyecto: una asignación KEY=valor por línea,
// comentarios con # y líneas vacías ignorados, comillas opcionales alrededor
// del valor. Es el mismo fichero que exporta el Makefile y que lee
// docker-compose; leerlo también aquí permite lanzar las apps y los tests desde
// sbt o el IDE sin pasar por make.
object DotEnv {

  def read(path: Path): Map[String, String] =
    if (!Files.exists(path)) Map.empty
    else
      Files.readAllLines(path).asScala.iterator
        .map(_.trim)
        .filter(line => line.nonEmpty && !line.startsWith("#") && line.contains("="))
        .map { line =>
          val Array(key, value) = line.split("=", 2)  // solo el primer "=": el valor puede llevar más (claves base64)
          key.trim -> value.trim.stripPrefix("\"").stripSuffix("\"")
        }
        .filter { case (key, _) => key.nonEmpty }
        .toMap
}
