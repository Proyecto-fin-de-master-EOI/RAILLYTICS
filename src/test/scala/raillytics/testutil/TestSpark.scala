package raillytics.testutil

import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._

object TestSpark {

  // Hadoop en Windows necesita winutils.exe para escribir en disco local
  // (RawLocalFileSystem invoca `winutils chmod` al crear ficheros y directorios).
  // HADOOP_HOME se define en el .env del proyecto: el Makefile lo exporta, pero
  // `sbt test` también se lanza desde el pre-push hook y desde el IDE, sin pasar
  // por make. En ese caso se lee del .env directamente. Debe fijarse antes de que
  // Hadoop cargue su clase Shell (que lo lee una sola vez), es decir, antes de
  // crear la SparkSession.
  val DotEnvFile: Path = Paths.get(".env")

  def configureHadoopHome(): Unit = {
    val isWindows = sys.props.getOrElse("os.name", "").startsWith("Windows")
    val unset = sys.props.get("hadoop.home.dir").isEmpty && sys.env.get("HADOOP_HOME").forall(_.isEmpty)
    if (isWindows && unset) {
      dotEnv(DotEnvFile).get("HADOOP_HOME").filter(_.nonEmpty) match {
        case Some(home) if Files.exists(Paths.get(home, "bin", "winutils.exe")) =>
          System.setProperty("hadoop.home.dir", home)
        case Some(home) =>
          System.err.println(s"AVISO: HADOOP_HOME=$home (.env) no contiene bin\\winutils.exe; " +
            "los tests que escriben en disco con Spark fallarán (ver README, sección Git hooks)")
        case None =>
          System.err.println("AVISO: HADOOP_HOME no está definido ni en el entorno ni en .env; " +
            "los tests que escriben en disco con Spark fallarán (ver README, sección Git hooks)")
      }
    }
  }

  // Lectura mínima de un .env (KEY=valor por línea; comentarios y líneas vacías se ignoran).
  private def dotEnv(path: Path): Map[String, String] =
    if (!Files.exists(path)) Map.empty
    else
      Files.readAllLines(path).asScala.iterator
        .map(_.trim)
        .filter(line => line.nonEmpty && !line.startsWith("#") && line.contains("="))
        .map { line =>
          val Array(key, value) = line.split("=", 2)
          key.trim -> value.trim.stripPrefix("\"").stripSuffix("\"")
        }
        .toMap

  // SparkSession local para las suites; una por suite (se para en afterAll).
  def session(appName: String): SparkSession = {
    configureHadoopHome()
    SparkSession.builder().appName(appName).master("local[1]").getOrCreate()
  }
}
