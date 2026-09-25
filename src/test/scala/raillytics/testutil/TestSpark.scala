package raillytics.testutil

import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}

object TestSpark {

  // Hadoop en Windows necesita winutils.exe para escribir en disco local
  // (RawLocalFileSystem invoca `winutils chmod` al crear ficheros y directorios).
  // El Makefile exporta HADOOP_HOME, pero `sbt test` también se lanza desde el
  // pre-push hook y desde el IDE, sin esa variable: en ese caso se usa la misma
  // ruta por defecto que el Makefile. Debe fijarse antes de que Hadoop cargue su
  // clase Shell (que la lee una sola vez), es decir, antes de crear la SparkSession.
  val DefaultWindowsHadoopHome = "C:\\dev\\winutils\\hadoop-3.0.0"

  def configureHadoopHome(): Unit = {
    val isWindows = sys.props.getOrElse("os.name", "").startsWith("Windows")
    val unset = sys.props.get("hadoop.home.dir").isEmpty && sys.env.get("HADOOP_HOME").forall(_.isEmpty)
    if (isWindows && unset) {
      if (Files.exists(Paths.get(DefaultWindowsHadoopHome, "bin", "winutils.exe")))
        System.setProperty("hadoop.home.dir", DefaultWindowsHadoopHome)
      else
        System.err.println(
          s"AVISO: HADOOP_HOME no está definido y no existe $DefaultWindowsHadoopHome\\bin\\winutils.exe; " +
            "los tests que escriben en disco con Spark fallarán (ver README, sección Git hooks)"
        )
    }
  }

  // SparkSession local para las suites; una por suite (se para en afterAll).
  def session(appName: String): SparkSession = {
    configureHadoopHome()
    SparkSession.builder().appName(appName).master("local[1]").getOrCreate()
  }
}
