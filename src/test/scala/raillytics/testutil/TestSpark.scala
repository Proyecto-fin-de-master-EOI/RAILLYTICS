package raillytics.testutil

import org.apache.spark.sql.SparkSession
import raillytics.common.config.{AppConfig, DotEnv}

import java.nio.file.{Files, Paths}

object TestSpark {

  // Hadoop en Windows necesita winutils.exe para escribir en disco local
  // (RawLocalFileSystem invoca `winutils chmod` al crear ficheros y directorios).
  // HADOOP_HOME se define en el .env del proyecto: el Makefile lo exporta, pero
  // `sbt test` también se lanza desde el pre-push hook y desde el IDE, sin pasar
  // por make. En ese caso se lee del .env directamente. Debe fijarse antes de que
  // Hadoop cargue su clase Shell (que lo lee una sola vez), es decir, antes de
  // crear la SparkSession.
  def configureHadoopHome(): Unit = {
    val isWindows = sys.props.getOrElse("os.name", "").startsWith("Windows")
    val unset = sys.props.get("hadoop.home.dir").isEmpty && sys.env.get("HADOOP_HOME").forall(_.isEmpty)
    if (isWindows && unset) {
      DotEnv.read(AppConfig.DotEnvFile).get("HADOOP_HOME").filter(_.nonEmpty) match {
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

  // SparkSession local para las suites; una por suite (se para en afterAll).
  def session(appName: String): SparkSession = {
    configureHadoopHome()
    SparkSession.builder().appName(appName).master("local[1]").getOrCreate()
  }
}
