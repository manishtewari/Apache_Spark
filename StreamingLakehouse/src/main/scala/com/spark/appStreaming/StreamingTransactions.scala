package com.spark.appStreaming

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.spark.appStreaming.PolarisBootstrap._
import com.spark.appStreaming.genricFunctions.DFHelpers

object StreamingTransactions {

  def main(args: Array[String]): Unit = {
     //Load Configs from configuration file
     val config: Config = ConfigFactory.load("streaming.conf")
     val iceberg_version = config.getString("iceberg_version")
     val polaris_version = config.getString("polaris_version")
     val iceberg_aws = config.getString("iceberg_aws")
     val region = config.getString("region")
     val polaris_catalog_uri = config.getString("polaris_catalog_uri")
     val POLARIS_MANAGEMENT = config.getString("POLARIS_MANAGEMENT")
     val access_key = config.getString("access_key")
     val secret_key = config.getString("secret_key")
     val CREDENTIAL = config.getString("CREDENTIAL")
     val CATALOG_NAMES  = List("warehouse")
     val BOOTSTRAP_PRINCIPAL = config.getString("BOOTSTRAP_PRINCIPAL")
     val APP_PRINCIPAL = config.getString("APP_PRINCIPAL")
    //Get the Auth token inorder to give permissions
     val token = authenticate()
     val authHeader = s"Bearer $token"

     CATALOG_NAMES.foreach(ele => ensureCatalog(ele,authHeader))
     val appCreds = ensurePrincipal(APP_PRINCIPAL, authHeader)

     manageRole(APP_PRINCIPAL,authHeader,BOOTSTRAP_PRINCIPAL,CATALOG_NAMES)

    //Setting the aws region for the executors
    System.setProperty("aws.region", s"$region")
    var spark: SparkSession = null
      CATALOG_NAMES.foreach { cat =>
             spark = (SparkSession.builder.appName("stream app").master("local[*]")
            .config("spark.jars.packages", s"$polaris_version,$iceberg_aws,$iceberg_version")
            .config(s"spark.sql.catalog.$cat", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(s"spark.sql.catalog.$cat", "org.apache.polaris.spark.SparkCatalog")
            .config(s"spark.sql.catalog.$cat.uri", s"$polaris_catalog_uri")
            .config(s"spark.sql.catalog.$cat.catalog-name", s"$cat")
            .config(s"spark.sql.catalog.$cat.warehouse", s"$cat")
            .config(s"spark.sql.catalog.$cat.credential", s"$CREDENTIAL")
            .config(s"spark.sql.catalog.$cat.scope", "PRINCIPAL_ROLE:ALL")
            .config(s"spark.sql.catalog.$cat.header.X-Iceberg-Access-Delegation", "vended-credentials")
            .config(s"spark.sql.catalog.$cat.token-refresh-enabled", "true")
            .config("spark.sql.defaultCatalog", s"$cat")
            .config(s"spark.sql.catalog.$cat.s3.endpoint", "http://localhost:9000")
            .config(s"spark.sql.catalog.$cat.s3.path-style-access", "true")
            .config(s"spark.sql.catalog.$cat.s3.region", s"$region")
            .config(s"spark.sql.catalog.$cat.s3.access-key-id", s"$access_key")
            .config(s"spark.sql.catalog.$cat.s3.secret-access-key", s"$secret_key")
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
            .config("spark.hadoop.fs.s3a.access.key", s"$access_key")
            .config("spark.hadoop.fs.s3a.secret.key", s"$secret_key")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.executorEnv.AWS_REGION", s"$region")
            .config("spark.driver.extraJavaOptions", s"-Daws.region=$region")
            .config("spark.executor.extraJavaOptions", s"-Daws.region=$region")
            .getOrCreate())
      }
      spark.sparkContext.setLogLevel("ERROR")
      // Schema of JSON data that we are expecting

      val jsonSchema =  StructType(Seq(StructField("transaction_id",StringType,true),
        StructField("buyer",StructType(Seq(StructField("buyer_id",LongType,true),
        StructField("city",StringType,true), StructField("name",StringType,true))),true),
        StructField("seller",StructType(Seq(StructField("platform",StringType,true),
        StructField("seller_id",LongType,true))),true),
        StructField("transaction_details",StructType(Seq(StructField("amount",DoubleType,true),
        StructField("currency",StringType,true),StructField("merchant",StringType,true),
        StructField("transaction_time",StringType,true))),true)))

      val broker = config.getString("broker")
      val topic = config.getString("topic")
      val offset = config.getString("offset")

      val msg = spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker).option("subscribe", topic)
        .option("startingOffsets", offset).load()

      val msgDf = msg.selectExpr("cast(value as string) as jsonString")

      // Clean and parse the JSON
      val cleanedDF = msgDf.withColumn(
        "jsonString",regexp_replace(col("jsonString"), "\\s+", "") // replace line breaks & multiple spaces
      )

      val transformDf = cleanedDF.withColumn("jsonData", from_json(col("jsonString"), jsonSchema))
        .select("jsonData.*")

      transformDf.printSchema()
      val flattenDf = transformDf.explodeColumns
      flattenDf.printSchema()

      //Show namespaces
    println("print spark configuration ")
    spark.conf.getAll
      .filter(_._1.contains("spark.sql.catalog"))
      .foreach(println)

     spark.sql("USE warehouse")
     spark.sql("SHOW NAMESPACES").show()
     spark.sql("show catalogs").show()
     spark.sql("create namespace if not exists warehouse.spark_demo")
     spark.sql("show namespaces").show()
     spark.sql("use namespace spark_demo")
     println("post namespace creation")
     spark.sql("""DROP TABLE IF EXISTS warehouse.spark_demo.customer""")

     spark.sql("""
                  CREATE TABLE IF NOT EXISTS warehouse.spark_demo.customer (
                      transaction_id STRING,
                      buyer_buyer_id LONG,
                      buyer_city STRING,
                      buyer_name STRING,
                      seller_platform STRING,
                      seller_seller_id LONG,
                      transaction_details_amount DOUBLE,
                      transaction_details_currency STRING,
                      transaction_details_merchant STRING,
                      transaction_details_transaction_time STRING
                  ) USING iceberg

                  """)

    spark.sql("show tables").show()
    println("post table creation")
    flattenDf.filter(col("transaction_id").isNotNull).writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("6 seconds"))
        .option("checkpointLocation", "E:/spark_checkpoint/spark_demo_customer")
        .toTable("warehouse.spark_demo.customer")
       .awaitTermination()

      println("count of records ===> "+spark.sql("SELECT * FROM customer").count())

    }


}
