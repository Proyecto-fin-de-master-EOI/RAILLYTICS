ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.raillytics"

lazy val sparkVersion = "4.2.0"

lazy val root = (project in file("."))
  .settings(
    name := "raillytics-spark-jobs",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql"  % sparkVersion,
      "org.apache.hadoop" % "hadoop-aws" % "3.5.0",
      "org.yaml"          % "snakeyaml"  % "2.7",
      "org.scalatest"    %% "scalatest"  % "3.2.20" % Test
    ),
    // Varias specs crean/paran su propio SparkSession; sbt corre las suites de test
    // en paralelo dentro de la misma JVM por defecto, y Spark solo permite un
    // SparkContext activo por JVM, lo que provoca fallos intermitentes al chocar
    // dos SparkSession concurrentes. Desactivamos el paralelismo de tests.
    Test / parallelExecution := false
  )
