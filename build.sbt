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
      "org.scalatest"    %% "scalatest"  % "3.2.19" % Test
    )
  )
