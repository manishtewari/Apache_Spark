package com.spark.appStreaming

import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StructType,StructField}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.spark.appStreaming.genricFunctions.DFHelpers

object StreamingTransactions {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("streamingTransaction")
      .config("spark.sql.shuffle.partitions",3).master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    val config: Config = ConfigFactory.load("streaming.conf")

    // Schema of JSON data that we are expecting

    val jsonSchema =  StructType(Seq(StructField("transaction_id",StringType,true),
      StructField("buyer",StructType(Seq(StructField("buyer_id",LongType,true),StructField("city",StringType,true), StructField("name",StringType,true))),true),
      StructField("seller",StructType(Seq(StructField("platform",StringType,true),StructField("seller_id",LongType,true))),true),
      StructField("transaction_details",StructType(Seq(StructField("amount",DoubleType,true),StructField("currency",StringType,true),StructField("merchant",StringType,true),StructField("transaction_time",StringType,true))),true)))

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

    flattenDf.writeTo()
      //flattenJson(transformDf,spark)
    flattenDf.writeStream.trigger(Trigger.ProcessingTime("10 seconds")).outputMode("append")
      .format("console").start().awaitTermination()

  }

}
