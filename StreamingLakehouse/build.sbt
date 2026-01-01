
name := "SparkStructuredStreaming"
organization := "self"
version := "0.1"
scalaVersion := "2.12.18"
autoScalaLibrary := false
val sparkVersion = "3.4.1"
val icebergVersion = "1.10.0"
val polarisVersion = "1.2.0-incubating"
val deltaVersion = "3.3.2"
val hadoopVersion = "3.3.2"

resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)
val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "com.typesafe" % "config" % "1.4.2",
  "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % icebergVersion,
  "org.apache.polaris" % "polaris-spark-3.5_2.12" % polarisVersion,
  "org.apache.iceberg" % "iceberg-aws-bundle" % icebergVersion,
  "io.delta" % "delta-spark_2.12" % deltaVersion,
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.500",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.1",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.1"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies

