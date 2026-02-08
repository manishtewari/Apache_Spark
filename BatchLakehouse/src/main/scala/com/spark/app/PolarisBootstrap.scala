package main.scala.com.spark.app

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

object PolarisBootstrap {
  val client = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(5))
    .build()

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def authenticate(): String = {
    val payload =
      s"grant_type=client_credentials" +
        s"&client_id=$admin_client_id" +
        s"&client_secret=$admin_client_secret" +
        s"&scope=PRINCIPAL_ROLE:ALL"

    println("payload === " + payload)
    for (_ <- 1 to 20) {
      try {
        val request = HttpRequest.newBuilder()
          .uri(URI.create(auth))
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

  // ==============================
  // AUTHENTICATION
  // ==============================

  def send(request: HttpRequest): HttpResponse[String] =
    client.send(request, HttpResponse.BodyHandlers.ofString())

  //Create Principal role, catalog role and attach them

  def manageRole(app_principal: String, authHeader: String, bootstrap_principal: String, catalog_names: List[String]) = {
    val ROLE_NAME = s"${app_principal}_role"
    val roleBody = s"""{ "principalRole": { "name": "$ROLE_NAME" } }"""
    post(s"$polaris_management/principal-roles", roleBody, authHeader)
    put(
      s"$polaris_management/principals/$bootstrap_principal/principal-roles",
      roleBody,
      authHeader
    )
    println(s"Assigned principal role '$ROLE_NAME' to '$app_principal'.")
    catalog_names.foreach { cat =>
      val catRole = s"${cat}_role"
      val catRoleBody = s"""{ "catalogRole": { "name": "$catRole" } }"""
      post(s"$polaris_management/catalogs/$cat/catalog-roles", catRoleBody, authHeader)
      put(s"$polaris_management/principal-roles/$ROLE_NAME/catalog-roles/$cat", catRoleBody, authHeader)
      val grantBody =
        """{ "grant": { "type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT" } }"""
      put(s"$polaris_management/catalogs/$cat/catalog-roles/$catRole/grants", grantBody, authHeader)
      println(s"Granted full access on '$cat'.")
    }

  }

  def post(url: String, body: String, authHeader: String): Unit =
    send(HttpRequest.newBuilder()
      .uri(URI.create(url))
      .header("Authorization", authHeader)
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build())

  def put(url: String, body: String, authHeader: String): Unit =
    send(HttpRequest.newBuilder()
      .uri(URI.create(url))
      .header("Authorization", authHeader)
      .header("Content-Type", "application/json")
      .PUT(HttpRequest.BodyPublishers.ofString(body))
      .build())

  def ensureCatalog(name: String, authHeader: String): Boolean = {
    println(s"URL value === " + s"$polaris_management/catalogs/$name")
    val getReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$polaris_management/catalogs/$name"))
      .header("Authorization", authHeader)
      .GET()
      .build()

    val getResp = send(getReq)
    if (getResp.statusCode() == 200) {
      println(s"Catalog '$name' already exists.")
      return true
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
      .uri(URI.create(s"$polaris_management/catalogs"))
      .header("Authorization", authHeader)
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()

    val postResp = send(postReq)
    println("Post resp==== " + postResp.statusCode())
    println(s"Post resp==== ${postResp.statusCode()} - ${postResp.body()}")

    postResp.statusCode() match {
      case 200 | 201 => {
        println(s"Catalog '$name' created.")
        false
      }
      case 409 => {println(s"Catalog '$name' already exists (409).")
        true }
      case _ => {
        println(s"Catalog '$name' creation failed: ${postResp.body()}")
        true
      }
    }
  }

  def ensurePrincipal(name: String, authHeader: String): Option[JsonNode] = {
    val getReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$polaris_management/principals/$name"))
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
      .uri(URI.create(s"$polaris_management/principals"))
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
        Some(creds)
      case 409 =>
        println(s"Principal '$name' already exists (409).")
        None
      case _ =>
        throw new RuntimeException(s"Failed to create principal: ${resp.body()}")
    }
  }

}
