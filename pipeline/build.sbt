import scala.collection.Seq

ThisBuild / version := "0.0.0"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "pipeline",
  )

lazy val sparkVersion = "3.4.1"

libraryDependencies ++= Seq(
  "io.spray" %% "spray-json" % "1.3.6",
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.crealytics" %% "spark-excel" % "3.4.1_0.19.0",
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.postgresql" % "postgresql" % "42.6.0"
)