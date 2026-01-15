package com.spark.appStreaming
object Constants {
  // The below map can be a oracle or any other rdbms db table as well

  val config: Config = ConfigFactory.load("streaming.conf")
  val iceberg_version = config.getString("iceberg_version")
  val polaris_version = config.getString("polaris_version")
  val iceberg_aws = config.getString("iceberg_aws")
  val endpoint_url = config.getString("minio_endpoint")
  val region = config.getString("region")
  val polaris_catalog_uri = config.getString("polaris_catalog_uri")
  val access_key = config.getString("access_key")
  val secret_key = config.getString("secret_key")
  val credential = config.getString("CREDENTIAL")
  val catalog_names = List("warehouse")
  val bootstrap_principal = config.getString("BOOTSTRAP_PRINCIPAL")
  val app_principal = config.getString("APP_PRINCIPAL")
  val polaris_management = config.getString("POLARIS_MANAGEMENT")
  val auth = config.getString("AUTH")
  val admin_client_id = config.getString("ADMIN_CLIENT_ID")
  val admin_client_secret = config.getString("ADMIN_CLIENT_SECRET")
  val broker = config.getString("broker")
  val topic = config.getString("topic")
  val offset = config.getString("offset")


  val jsonSchema =  StructType(Seq(StructField("transaction_id",StringType,true),
    StructField("buyer",StructType(Seq(StructField("buyer_id",LongType,true),
      StructField("city",StringType,true), StructField("name",StringType,true))),true),
    StructField("seller",StructType(Seq(StructField("platform",StringType,true),
      StructField("seller_id",LongType,true))),true),
    StructField("transaction_details",StructType(Seq(StructField("amount",DoubleType,true),
      StructField("currency",StringType,true),StructField("merchant",StringType,true),
      StructField("transaction_time",StringType,true))),true)))



}
