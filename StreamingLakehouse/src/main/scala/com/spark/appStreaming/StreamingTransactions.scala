package com.spark.appStreaming

object StreamingTransactions {

  def main(args: Array[String]): Unit = {
    //Get the Auth token inorder to give permissions
    val token = authenticate()
    var catalogExists = false
    val authHeader = s"Bearer $token"

    catalog_names.foreach(ele =>  catalogExists = ensureCatalog(ele, authHeader))
    //Setting the aws region for the executors
    System.setProperty("aws.region", s"$region")
    var spark: SparkSession = null
    catalog_names.foreach { catalog_name =>
      spark = createSparkSession(polaris_version, iceberg_aws, iceberg_version, polaris_catalog_uri, catalog_name, credential, region, access_key, secret_key, endpoint_url)
    }
    spark.sparkContext.setLogLevel("ERROR")
    // Schema of JSON data that we are expecting
    if (!catalogExists)
    {
      manageRole(app_principal, authHeader, bootstrap_principal, catalog_names)
    }
    performDDL(spark,"warehouse","spark_demo","customer")
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

    println("print spark configuration ")
    spark.conf.getAll
      .filter(_._1.contains("spark.sql.catalog"))
      .foreach(println)

    flattenDf.filter(col("transaction_id").isNotNull).writeStream
      .format("iceberg")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("6 seconds"))
      .option("checkpointLocation", "s3a://warehouse/checkpoint_dir/customer")
      .toTable("warehouse.spark_demo.customer")
      .awaitTermination()

    println("count of records ===> "+spark.sql("SELECT * FROM customer").count())

  }
}
