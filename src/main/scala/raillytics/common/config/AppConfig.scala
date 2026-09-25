package raillytics.common.config

import com.typesafe.config.{Config, ConfigFactory, ConfigResolveOptions}

import java.nio.file.{Path, Paths}
import scala.jdk.CollectionConverters._

// Configuración de las apps Scala (Typesafe Config). Las claves y sus valores
// por defecto están en src/main/resources/application.conf; aquí solo se monta
// la pila de fuentes, de mayor a menor precedencia:
//   1. propiedades del sistema (-Dclave=valor en la JVM)
//   2. variables de entorno (el Makefile las exporta desde el .env)
//   3. el .env del proyecto, leído directamente (para sbt o el IDE sin make)
//   4. application.conf (valores por defecto)
// application.conf referencia las variables como ${?NOMBRE}: se resuelven
// contra esta pila, así que una variable definida en cualquiera de las tres
// primeras fuentes sustituye al valor por defecto.
object AppConfig {

  // Relativo al directorio de trabajo: la raíz del repo, desde donde se lanza sbt.
  val DotEnvFile: Path = Paths.get(".env")

  def load(dotEnv: Path = DotEnvFile): Config =
    // parseProperties lee las propiedades actuales de la JVM; systemProperties()
    // cachea la primera lectura y no vería una -D fijada después de arrancar.
    ConfigFactory.parseProperties(System.getProperties)
      .withFallback(ConfigFactory.systemEnvironment())
      .withFallback(ConfigFactory.parseMap(DotEnv.read(dotEnv).asJava, "fichero .env"))
      .withFallback(ConfigFactory.parseResources("application.conf"))
      .resolve()

  // Solo application.conf, más los overrides que se pasen en HOCON (p. ej.
  // "MINIO_BUCKET_GOLD = otro"), sin entorno ni .env: para tests deterministas.
  def defaults(overrides: String = ""): Config =
    ConfigFactory.parseString(overrides)
      .withFallback(ConfigFactory.parseResources("application.conf"))
      .resolve(ConfigResolveOptions.noSystem())
}
