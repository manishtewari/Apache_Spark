package com.spark.appStreaming

object genricFunctions {
  implicit class DFHelpers(df: DataFrame) {
    def columns = {
      val dfColumns = df.columns.map(_.toLowerCase)
      df.schema.fields.flatMap { data =>
        data match {
          case column if column.dataType.isInstanceOf[StructType] => {
            column.dataType.asInstanceOf[StructType].fields.map { field =>
              val columnName = column.name
              val fieldName = field.name
              col(s"${columnName}.${fieldName}").as(s"${columnName}_${fieldName}")
            }.toList
          }
          case column => List(col(s"${column.name}"))
        }
      }
    }

    def flatten: DataFrame = {
      val empty = df.schema.filter(_.dataType.isInstanceOf[StructType]).isEmpty
      empty match {
        case false =>
          df.select(columns: _*).flatten
        case _ => df
      }
    }
    //Tail rec function to keep exploding the array fields
    def explodeColumns = {
      @tailrec
      def columns(cdf: DataFrame): DataFrame = cdf.schema.fields.filter(_.dataType.typeName == "array") match {
        case c if !c.isEmpty => columns(c.foldLeft(cdf)((dfa, field) => {
          dfa.withColumn(field.name, explode_outer(col(s"${field.name}"))).flatten
        }))
        case _ => cdf
      }

      columns(df.flatten)
    }
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

  def performDDL(spark: SparkSession,catalog: String, namespace: String, tableName: String):Unit={
    spark.sql(s"USE ${catalog}")
    spark.sql(s"create namespace if not exists ${catalog}.${namespace}")
    spark.sql("show namespaces").show()
    spark.sql("use namespace spark_demo")
    println("post namespace creation")
    spark.sql(s"""DROP TABLE IF EXISTS ${catalog}.${namespace}.${tableName}""")
    spark.sql(s"""CREATE TABLE IF NOT EXISTS ${catalog}.${namespace}.${tableName} (
    transaction_id STRING,
    buyer_buyer_id LONG,
    buyer_city STRING,
    buyer_name STRING,
    seller_platform STRING,
    seller_seller_id LONG,
    transaction_details_amount DOUBLE,
    transaction_details_currency STRING,
    transaction_details_merchant STRING,
    transaction_details_transaction_time STRING)
     USING iceberg""" )

    spark.sql("show tables").show()

  }

}