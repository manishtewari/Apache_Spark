
name := "SparkStructuredStreaming"
organization := "self"
version := "0.1"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.2"
val icebergVersion = "1.10.0"
val polarisVersion = "1.2.0-incubating"
val hadoopVersion = "3.3.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",

  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "com.typesafe" % "config" % "1.4.2",
  "org.apache.polaris" % "polaris-spark-3.5_2.12" % polarisVersion,

  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.1",

  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % icebergVersion % "provided",
  "org.apache.iceberg" % "iceberg-aws-bundle" % icebergVersion % "provided",
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion % "provided"
)
