package main.scala.com.spark.app
import com.typesafe.config.{Config, ConfigFactory}

object Constants {
  // The below map can be a oracle or any other rdbms db table as well

  val vendorMap = Map(
    1 -> "Creative Mobile Technologies, LLC",
    2 -> "Curb Mobility, LLC",
    6 -> "Myle Technologies Inc",
    7 -> "Helix"
  )

  val rateCodeID = Map(
    1 -> "Standard rate",
    2 -> "JFK",
    3 -> "Newark",
    4 -> "Nassau or Westchester",
    5 -> "Negotiated fare" ,
    6 -> "Group ride",
    99 -> "None"
  )

  val store_and_fwd_flag = Map (
    "Y" -> "store and forward trip",
    "N"  -> "not a store and forward trip"
  )

  val payment_type = Map (
    0 -> "Flex Fare trip",
    1 -> "Credit card",
    2 -> "Cash",
    3 -> "No charge",
    4 -> "Dispute",
    5 -> "Unknown",
    6 -> "Voided trip"
  )

  val config: Config = ConfigFactory.load("batch.conf")
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


}
