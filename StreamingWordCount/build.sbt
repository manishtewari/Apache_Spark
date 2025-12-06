
name := "SparkStructuredStreaming"
organization := "guru.learningjournal"
version := "0.1"
scalaVersion := "2.13.17"
autoScalaLibrary := false
val sparkVersion = "3.5.0"
val icebergVersion = "1.5.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "com.typesafe" % "config" % "1.4.2",
  "org.apache.iceberg" % "iceberg-spark-runtime-"% sparkVersion % icebergVersion
  // Adjust versions

    //spark-sql-kafka-0-10_2.13
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies

