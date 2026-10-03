ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.raillytics"

lazy val sparkVersion = "4.2.0"

// HADOOP_HOME del entorno (make lo exporta) o, si no viene, del .env del
// proyecto (pre-push hook, sbt a mano). Solo hace falta en Windows.
lazy val hadoopHome: Option[String] =
  sys.env.get("HADOOP_HOME").filter(_.nonEmpty).orElse {
    val dotEnv = file(".env")
    if (!dotEnv.exists) None
    else IO.readLines(dotEnv).map(_.trim)
      .collectFirst { case l if l.startsWith("HADOOP_HOME=") => l.stripPrefix("HADOOP_HOME=").trim.stripPrefix("\"").stripSuffix("\"") }
      .filter(_.nonEmpty)
  }

// Opciones de .jvmopts (sin comentarios ni líneas vacías): solo se aplican a la
// JVM de sbt, así que hay que repetirlas en la JVM de los tests.
lazy val jvmOpts: Seq[String] =
  IO.readLines(file(".jvmopts")).map(_.trim).filter(l => l.nonEmpty && !l.startsWith("#"))

lazy val root = (project in file("."))
  .settings(
    name := "raillytics-spark-jobs",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql"  % sparkVersion,
      "org.apache.hadoop" % "hadoop-aws" % "3.5.0",
      "org.yaml"          % "snakeyaml"  % "2.7",
      // Configuración de las apps (src/main/resources/application.conf, HOCON).
      "com.typesafe"      % "config"     % "1.4.9",
      "org.scalatest"    %% "scalatest"  % "3.2.20" % Test
    ),
    // Varias specs crean/paran su propio SparkSession; sbt corre las suites de test
    // en paralelo dentro de la misma JVM por defecto, y Spark solo permite un
    // SparkContext activo por JVM, lo que provoca fallos intermitentes al chocar
    // dos SparkSession concurrentes. Desactivamos el paralelismo de tests.
    Test / parallelExecution := false,
    // En Windows, Hadoop llama a código nativo de hadoop.dll (NativeIO$Windows)
    // al listar/escribir en disco local; sin él, las escrituras de Spark fallan
    // con UnsatisfiedLinkError. java.library.path no se puede cambiar con la JVM
    // ya arrancada, así que los tests corren en una JVM aparte que lo incluye.
    Test / fork := true,
    Test / javaOptions ++= jvmOpts ++ hadoopHome.toSeq.flatMap { home =>
      Seq(
        s"-Dhadoop.home.dir=$home",
        s"-Djava.library.path=${file(home) / "bin"}${java.io.File.pathSeparator}${sys.props.getOrElse("java.library.path", "")}"
      )
    },
    // Java fijado en el 21 para las JVM de los tests y de `runMain`. Spark 4.2 no
    // arranca en JDK recientes como el 27 (el que instala Homebrew por defecto):
    // org.apache.spark.unsafe.Platform busca por reflexión jdk.internal.ref.Cleaner,
    // que ya no existe, y todo SparkSession falla con ClassNotFoundException. Como
    // ambas JVM van en fork, usan el JDK 21 aunque sbt corra con otro; sbt lo
    // localiza solo (fullJavaHomes: /Library/Java/JavaVirtualMachines, /usr/lib/jvm,
    // sdkman...). Si no lo encuentra se usa el JDK de sbt, que vale si ya es el 21.
    Test / javaHome := {
      val jdk21   = fullJavaHomes.value.get("21")
      val sbtJava = sys.props("java.specification.version")
      if (jdk21.isEmpty && sbtJava != "21")
        sLog.value.warn(s"No se encuentra un JDK 21 y sbt corre con Java $sbtJava: Spark 4.2 necesita " +
          "Java 21 (instálalo o apunta JAVA_HOME a él); con otro JDK los tests y runMain pueden fallar " +
          "(p. ej. ClassNotFoundException: jdk.internal.ref.Cleaner).")
      jdk21
    },
    // Lo mismo para `runMain` (make 01_raw-uploader ... 05_gold). Sin fork, Spark
    // corre dentro de la JVM de sbt con classloader en capas: sbt copia los jars a
    // target/bg-jobs/sbt_<id>/ y borra ese directorio al salir, justo antes de que
    // el shutdown hook de Hadoop lea core-default.xml del jar ya borrado
    // (NoSuchFileException al final de cada job, aunque el job haya ido bien).
    // En una JVM aparte no hay nada que sbt borre mientras Hadoop sigue vivo.
    run / fork := true,
    run / javaOptions := (Test / javaOptions).value,
    run / javaHome := (Test / javaHome).value,
    run / connectInput := true
  )
