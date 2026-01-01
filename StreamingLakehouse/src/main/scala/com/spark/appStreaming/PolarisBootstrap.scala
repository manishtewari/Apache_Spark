package com.spark.appStreaming

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{Config, ConfigFactory}

object PolarisBootstrap {

  // CONFIGURATION SECTION
  val config: Config = ConfigFactory.load("streaming.conf")
  val POLARIS_MANAGEMENT = config.getString("POLARIS_MANAGEMENT")
  val AUTH = config.getString("AUTH")
  val ADMIN_CLIENT_ID = config.getString("ADMIN_CLIENT_ID")
  val ADMIN_CLIENT_SECRET = config.getString("ADMIN_CLIENT_SECRET")

  // ==============================
  // HTTP + JSON SETUP
  // ==============================

  val client = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(5))
    .build()

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def send(request: HttpRequest): HttpResponse[String] =
    client.send(request, HttpResponse.BodyHandlers.ofString())

  // ==============================
  // AUTHENTICATION
  // ==============================

  def authenticate(): String = {
    val payload =
      s"grant_type=client_credentials" +
        s"&client_id=$ADMIN_CLIENT_ID" +
        s"&client_secret=$ADMIN_CLIENT_SECRET" +
        s"&scope=PRINCIPAL_ROLE:ALL"

    println("payload === "+ payload)
    for (_ <- 1 to 20) {
      try {
        val request = HttpRequest.newBuilder()
          .uri(URI.create(AUTH))
          .header("Content-Type", "application/x-www-form-urlencoded")
          .POST(HttpRequest.BodyPublishers.ofString(payload))
          .build()

        val response = send(request)
        if (response.statusCode() == 200) {
          println("Authenticated as admin.")
          val json = mapper.readTree(response.body())
          return json.get("access_token").asText()
        }
      } catch {
        case _: Exception => // retry
      }
      Thread.sleep(3000)
    }
    throw new RuntimeException("Polaris not ready or authentication failed.")
  }

  //Create Principal role, catalog role and attach them

  def manageRole(app_principal: String, authHeader:String, bootstrap_principal: String, catalog_names: List[String])={
    val ROLE_NAME = s"${app_principal}_role"
    val roleBody = s"""{ "principalRole": { "name": "$ROLE_NAME" } }"""
    post(s"$POLARIS_MANAGEMENT/principal-roles", roleBody, authHeader)
    put(
      s"$POLARIS_MANAGEMENT/principals/$bootstrap_principal/principal-roles",
      roleBody,
      authHeader
    )
    println(s"Assigned principal role '$ROLE_NAME' to '$app_principal'.")
    catalog_names.foreach { cat =>
      val catRole = s"${cat}_role"
      val catRoleBody = s"""{ "catalogRole": { "name": "$catRole" } }"""
      post(s"$POLARIS_MANAGEMENT/catalogs/$cat/catalog-roles", catRoleBody,authHeader)
      put(s"$POLARIS_MANAGEMENT/principal-roles/$ROLE_NAME/catalog-roles/$cat", catRoleBody,authHeader)
      val grantBody =
        """{ "grant": { "type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT" } }"""
      put(s"$POLARIS_MANAGEMENT/catalogs/$cat/catalog-roles/$catRole/grants", grantBody,authHeader)
      println(s"Granted full access on '$cat'.")
    }

  }

  // ==============================
  // 1. ENSURE CATALOG EXISTS
  // ==============================

  def ensureCatalog(name: String,authHeader: String): Unit = {
    println(s"URL value === "+s"$POLARIS_MANAGEMENT/catalogs/$name")
    val getReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$POLARIS_MANAGEMENT/catalogs/$name"))
      .header("Authorization", authHeader)
      .GET()
      .build()

    val getResp = send(getReq)
    if (getResp.statusCode() == 200) {
      println(s"Catalog '$name' already exists.")
      return
    }

    val body =
      s"""
         |{
         |    "name": "$name",
         |    "type": "INTERNAL",
         |    "properties": {
         |      "default-base-location": "s3://$name",
         |      "s3.endpoint": "http://minio:9000",
         |      "s3.access-key-id": "admin",
         |      "s3.secret-access-key": "password",
         |      "s3.path-style-access": "true",
         |      "s3.region": "us-east-1"
         |    },
         |    "storageConfigInfo": {
         |      "roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role",
         |      "storageType": "S3",
         |      "allowedLocations": ["s3://$name/*"]
         |    }
         |}
         |""".stripMargin

    val postReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$POLARIS_MANAGEMENT/catalogs"))
      .header("Authorization", authHeader)
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()

    val postResp = send(postReq)
    println("Post resp==== "+postResp.statusCode() )
    println(s"Post resp==== ${postResp.statusCode()} - ${postResp.body()}")

    postResp.statusCode() match {
      case 200 | 201 => println(s"Catalog '$name' created.")
      case 409       => println(s"Catalog '$name' already exists (409).")
      case _         => println(s"Catalog '$name' creation failed: ${postResp.body()}")
    }
  }


  def ensurePrincipal(name: String,authHeader: String): Option[JsonNode] = {
    val getReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$POLARIS_MANAGEMENT/principals/$name"))
      .header("Authorization", authHeader)
      .GET()
      .build()

    if (send(getReq).statusCode() == 200) {
      println(s"Principal '$name' already exists.")
      return None
    }

    val body =
      s"""
         |{
         |  "principal": {
         |    "name": "$name",
         |    "properties": { "purpose": "demo" }
         |  },
         |  "credentialRotationRequired": false
         |}
         |""".stripMargin

    val postReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$POLARIS_MANAGEMENT/principals"))
      .header("Authorization", authHeader)
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()

    val resp = send(postReq)
    resp.statusCode() match {
      case 201 =>
        val json = mapper.readTree(resp.body())
        val creds = json.get("credentials")
        println(s"Created principal '$name'.")
        println(s"  clientId: ${creds.get("clientId").asText()}")
        println(s"  clientSecret: ${creds.get("clientSecret").asText()}")
        Some(creds)
      case 409 =>
        println(s"Principal '$name' already exists (409).")
        None
      case _ =>
        throw new RuntimeException(s"Failed to create principal: ${resp.body()}")
    }
  }

  def post(url: String, body: String,authHeader : String): Unit =
    send(HttpRequest.newBuilder()
      .uri(URI.create(url))
      .header("Authorization", authHeader)
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build())

  def put(url: String, body: String,authHeader : String): Unit =
    send(HttpRequest.newBuilder()
      .uri(URI.create(url))
      .header("Authorization", authHeader)
      .header("Content-Type", "application/json")
      .PUT(HttpRequest.BodyPublishers.ofString(body))
      .build())

}
