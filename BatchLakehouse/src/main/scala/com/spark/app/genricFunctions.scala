package main.scala.com.spark.app

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import io.minio.MinioClient
import io.minio.PutObjectArgs
import java.net.URL
import java.io.{InputStream,ByteArrayInputStream,IOException}
import scala.util.{Try, Success, Failure}

object genricFunctions {
  def readParquet( Path: String, spark: SparkSession): DataFrame = {
    spark.read.format("parquet").load(Path)
  }

  def readCsv( Path: String, spark: SparkSession): DataFrame = {
    spark.read.format("csv").option("header","true").option("inferSchema","true").load(Path)
  }

  def createSparkSession(polaris_version: String, iceberg_aws: String, iceberg_version: String, polaris_catalog_uri: String, catalog_name : String, credential: String, region: String, access_key: String, secret_key : String, endpoint_url : String): SparkSession = {
    val spark = SparkSession.builder.appName("stream app").master("local[*]")
      .config("spark.jars.packages", s"$polaris_version,$iceberg_aws,$iceberg_version")
      .config(s"spark.sql.catalog.$catalog_name", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config(s"spark.sql.catalog.$catalog_name", "org.apache.polaris.spark.SparkCatalog")
      .config(s"spark.sql.catalog.$catalog_name.uri", s"$polaris_catalog_uri")
      .config(s"spark.sql.catalog.$catalog_name.catalog-name", s"$catalog_name")
      .config(s"spark.sql.catalog.$catalog_name.warehouse", s"$catalog_name")
      .config(s"spark.sql.catalog.$catalog_name.credential", s"$credential")
      .config(s"spark.sql.catalog.$catalog_name.scope", "PRINCIPAL_ROLE:ALL")
      .config(s"spark.sql.catalog.$catalog_name.header.X-Iceberg-Access-Delegation", "vended-credentials")
      .config(s"spark.sql.catalog.$catalog_name.token-refresh-enabled", "true")
      .config("spark.sql.defaultCatalog", s"$catalog_name")
      .config(s"spark.sql.catalog.$catalog_name.s3.endpoint", s"$endpoint_url")
      .config(s"spark.sql.catalog.$catalog_name.s3.path-style-access", "true")
      .config(s"spark.sql.catalog.$catalog_name.s3.region", s"$region")
      .config(s"spark.sql.catalog.$catalog_name.s3.access-key-id", s"$access_key")
      .config(s"spark.sql.catalog.$catalog_name.s3.secret-access-key", s"$secret_key")
      .config("spark.hadoop.fs.s3a.endpoint", s"$endpoint_url")
      .config("spark.hadoop.fs.s3a.access.key", s"$access_key")
      .config("spark.hadoop.fs.s3a.secret.key", s"$secret_key")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.executorEnv.AWS_REGION", s"$region")
      .config("spark.driver.extraJavaOptions", s"-Daws.region=$region")
      .config("spark.executor.extraJavaOptions", s"-Daws.region=$region")
      .getOrCreate()
    spark
  }

  def getUrlFolder(fileName : String,file_url: String): (String,String) ={
    val key: String = fileName.split(",")(0)
    var url : String = null
    var folder: String = null
    println("called for file "+ fileName)
    if (key == "data"){
      url = file_url+"trip-data/"+fileName.split(",")(1)
      folder = fileName.split(",")(0)
    }
    else {
      folder = fileName.split(",")(0)
      url = file_url+"misc/"+fileName.split(",")(1)
    }
    (url,folder)
  }

  def dataDownload(urlToDownload: String, minoEndpoint : String, user: String, password: String,bucketName: String,folderName :String): Boolean = {
    //val folderName = "data"
    println("In data download")
    val minioClient = MinioClient.builder()
      .endpoint(minoEndpoint)
      .credentials(user,password)
      .build()

    // Stream data from URL to MinIO
    Try {
      val url = new URL(urlToDownload)
      println("In data download in try")
      val connection = url.openConnection()
      connection.setConnectTimeout(15000)
      connection.setReadTimeout(12000)

      val httpConn = connection.asInstanceOf[java.net.HttpURLConnection]
      if (httpConn.getResponseCode != 200)
        {
          println(s"In exception block ${httpConn.getResponseCode} - ${httpConn.getResponseMessage} " )
          throw new IOException(s"HTTP ${httpConn.getResponseCode} - ${httpConn.getResponseMessage}")
        }
      println("response code "+httpConn.getResponseCode )
      val inputStream: InputStream = httpConn.getInputStream
      val contentLength = httpConn.getContentLengthLong
      println("length "+ httpConn.getContentLengthLong)

      val fileName = urlToDownload.split("/").last
      val objectKey = if (folderName.isEmpty) fileName else s"${folderName}/${fileName}"

      try {
        val emptyStream = new ByteArrayInputStream(new Array[Byte](0))
        val putObjectArgs = PutObjectArgs.builder()
          .bucket(bucketName)
          .`object`(objectKey)
          .stream(inputStream, contentLength, -1)
          .contentType("application/x-directory")
          .build()
        val response = minioClient.putObject(putObjectArgs)
        println(s"${response.toString}")
        println(s"Successfully uploaded object ${response.`object`()} to bucket ${response.bucket()} with ETag ${response.etag()}")
      }
        catch {
          case e: Exception => e.printStackTrace()
        }
      finally {
        // Ensure the input stream is closed after use
        if (inputStream != null) inputStream.close()
      }
    } match {
      case Success(_) => return true
      case Failure(e) => return false
    }
  }

  def enrich(lookupMap : Map[Int,String]) ={
    udf((vendorId: Int) => lookupMap.get(vendorId))
  }

  def enrichStore(lookupMap : Map[String,String]) ={
    udf((storeName: String) => lookupMap.get(storeName))
  }

  def performDDL(spark: SparkSession,catalog: String, namespace: String, tableName: String):Unit={
    spark.sql(s"USE ${catalog}")
    spark.sql(s"create namespace if not exists ${catalog}.${namespace}")
    spark.sql("show namespaces").show()
    spark.sql("use namespace spark_demo")
    println("post namespace creation")
    spark.sql(s"""DROP TABLE IF EXISTS ${catalog}.${namespace}.${tableName}""")
    spark.sql(s"""CREATE TABLE IF NOT EXISTS ${catalog}.${namespace}.${tableName} (
                 |    tpep_pickup_datetime TIMESTAMP_NTZ,
                 |    tpep_dropoff_datetime TIMESTAMP_NTZ,
                 |    passenger_count LONG,
                 |    trip_distance DOUBLE,
                 |    PULocationID INT,
                 |    DOLocationID INT,
                 |    fare_amount DOUBLE,
                 |    extra DOUBLE,
                 |    mta_tax DOUBLE,
                 |    tip_amount DOUBLE,
                 |    tolls_amount DOUBLE,
                 |    improvement_surcharge DOUBLE,
                 |    total_amount DOUBLE,
                 |    congestion_surcharge DOUBLE,
                 |    Airport_fee DOUBLE,
                 |    cbd_congestion_fee DOUBLE,
                 |    vendorName STRING,
                 |    rateCodeName STRING,
                 |    paymentMethod STRING,
                 |    storeFwd STRING,
                 |    Pickup_Borough STRING,
                 |    Pickup_Zone STRING,
                 |    Pickup_Service_Zone STRING,
                 |    DropOff_Borough STRING,
                 |    DropOff_Zone STRING,
                 |    DropOff_Service_Zone STRING
                 |)
                 |USING iceberg
                 |PARTITIONED BY (cobdate Date);""".stripMargin)

    spark.sql("show tables").show()

  }
}