package com.spark.app
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.spark.app.PolarisBootstrap._
import com.spark.app.Constants._
import com.spark.app.genricFunctions._

object NycTaxiDataAnalysis {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println("Usage: NYCTaxiIngestion <file_url> <file_name> ")
      sys.exit(1)
    }
    //Load Configs from configuration file

    val file_url = args(0)
    val file_names = args(1)
    val data_reload = args(2).toBoolean

    val token = authenticate()
    val authHeader = s"Bearer $token"
    var catalogExists = false
    val bucketName = catalog_names.head
    var dataLoaded = false // data downloaded flag

    catalog_names.foreach(ele =>  catalogExists = ensureCatalog(ele, authHeader))

    //Setting the aws region for the executors
    System.setProperty("aws.region", s"$region")
    var spark: SparkSession = null
    catalog_names.foreach { catalog_name =>
      spark = createSparkSession(polaris_version, iceberg_aws, iceberg_version, polaris_catalog_uri, catalog_name, credential, region, access_key, secret_key, endpoint_url)
    }
    spark.sparkContext.setLogLevel("ERROR")
    println("debug  " + file_names.split("\\|").head)
    if (data_reload){
      file_names.split("\\|").foreach(file_name => {
        println("calling for file "+ file_name)
        val params: (String, String) = getUrlFolder(file_name, file_url)
        println(params._1+ " and "+params._2)
        dataLoaded = dataDownload(params._1, endpoint_url , access_key, secret_key,bucketName,params._2)
      })
    }

    if (!catalogExists)
    {
      //log.info("Give access to newly created catalog only and perform DDL operation")
      manageRole(app_principal, authHeader, bootstrap_principal, catalog_names)
      performDDL(spark,"warehouse","spark_demo","nyc_traffic")
      //
      if (!dataLoaded) {
        file_names.split("\\|").foreach(file_name => {
          println("calling for file " + file_name)
          val params: (String, String) = getUrlFolder(file_name, file_url)
          println(params._1 + " and " + params._2)
          dataLoaded = dataDownload(params._1, endpoint_url, access_key, secret_key, bucketName, params._2)
        })
      }
      if (!dataLoaded)
      {
        println("No data to process")
        sys.exit(1)
      }
    }

    println("print spark configuration ")
    spark.conf.getAll.filter(_._1.contains("spark.sql.catalog")).foreach(println)
    val tripPath = s"s3a://$bucketName//data"
    val taxiZonePath = s"s3a://$bucketName//misc"
    val yellow_trip_df =readParquet(tripPath,spark)
    val taxi_zone_df =readCsv(taxiZonePath,spark)

    val yellowTripEnriched = yellow_trip_df.withColumn("vendorName",enrich(vendorMap)(col("VendorID")))
      .withColumn("rateCodeName",enrich(rateCodeID)(col("RatecodeID")))
      .withColumn("paymentMethod",enrich(payment_type)(col("payment_type")))
      .withColumn("storeFwd",enrichStore(store_and_fwd_flag)(col("store_and_fwd_flag")))
      .withColumn("cobdate",to_date(col("tpep_pickup_datetime")))
      .drop("payment_type","RatecodeID","VendorID","store_and_fwd_flag")

    val tripPickZoneDf = yellowTripEnriched.join(taxi_zone_df,
        yellowTripEnriched.col("PULocationID") === taxi_zone_df.col("LocationID")
        ,"left").withColumn("Pickup_Borough",col("Borough")).withColumn("Pickup_Zone",col("Zone"))
      .withColumn("Pickup_Service_Zone",col("service_zone"))
      .drop("LocationId","Borough","Zone","service_zone")

    val tripDropZoneDf = tripPickZoneDf.join(taxi_zone_df,
        yellowTripEnriched.col("DOLocationID") === taxi_zone_df.col("LocationID")
        ,"left").withColumn("DropOff_Borough",col("Borough")).withColumn("DropOff_Zone",col("Zone"))
      .withColumn("DropOff_Service_Zone",col("service_zone"))
      .drop("LocationId","Borough","Zone","service_zone")

    println("count of records in trip ===> " +tripDropZoneDf.count())
    println("taxi zone df "+ taxi_zone_df.count())

    // create cobdate from timestamp field and partition
    println("post table creation")
    // Table will be created if its not there , the namespace spark_demo should be present

    tripDropZoneDf.writeTo("warehouse.spark_demo.nyc_traffic")
      .using("iceberg").partitionedBy(col("cobdate"))
      .createOrReplace()

    spark.sql("select count(*) from warehouse.spark_demo.nyc_traffic").show()


  }


}
