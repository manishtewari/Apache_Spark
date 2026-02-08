
# We can use the docker.yml file to set up the env for the streaming application, it has below services
1. Kafka-broker
2. Polaris
3. Trino
4. Minio
5. Minio-Client

# Create iceberg properties file for trino
mkdir -p ./trino/catalog/

cat << 'EOF' > ./trino/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris:8181/api/catalog/
iceberg.rest-catalog.warehouse=warehouse
iceberg.rest-catalog.vended-credentials-enabled=true
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=root:secret
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
# Required for Trino to read from/write to S3
fs.native-s3.enabled=true 
s3.endpoint=http://minio:9000 
s3.region=us-east-1
EOF

# We can execute below command to start the trino shell 
docker compose exec -it trino trino --server localhost:8080 --catalog iceberg

# Start the Docker Composer
docker compose up

# Post docker command we can access Trino, Minio in below URL
Trino Web UI: http://localhost:8080 

MinIO UI: http://localhost:9001 (admin/password) 

MinIO API: http://localhost:9000

Polaris: http://localhost:8181


# Steps we can perform on the shell, the application does the same steps in the PolarisBootstrap class, please refer it for scala way of implementing the same.
# Create access token 

ACCESS_TOKEN=$(curl -X POST 
http://localhost:8181/api/catalog/v1/oauth/tokens -d \
'grant_type=client_credentials&client_id=root&client_secret=secret&scope=PRINCIPAL_ROLE:ALL' | jq -r '.access_token')

# Use above access token while creating the catalog, which is created with various other details like storage location.

curl -i -X POST \
-H "Authorization: Bearer $ACCESS_TOKEN" \
http://localhost:8181/api/management/v1/catalogs \
--json '{
"name": "warehouse",
"type": "INTERNAL",
"properties": {
"default-base-location": "s3://warehouse",
"s3.endpoint": "http://minio:9000",
"s3.path-style-access": "true",
"s3.access-key-id": "admin",
"s3.secret-access-key": "password",
"s3.region": "us-east-1"
},
"storageConfigInfo": {
"roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role",
"storageType": "S3",
"allowedLocations": [
"s3://warehouse/*"
]
}
}'

# Verify if the catalog was created correctly

curl -X GET http://localhost:8181/api/management/v1/catalogs \
-H "Authorization: Bearer $ACCESS_TOKEN" | jq

# Create a catalog admin role
curl -X PUT http://localhost:8181/api/management/v1/catalogs/polariscatalog/catalog-roles/catalog_admin/grants \
-H "Authorization: Bearer $ACCESS_TOKEN" \
--json '{"grant":{"type":"catalog", "privilege":"CATALOG_MANAGE_CONTENT"}}'

# Create a data engineer role
curl -X POST http://localhost:8181/api/management/v1/principal-roles \
-H "Authorization: Bearer $ACCESS_TOKEN" \
--json '{"principalRole":{"name":"data_engineer"}}'

# Connect the roles
curl -X PUT http://localhost:8181/api/management/v1/principal-roles/data_engineer/catalog-roles/polariscatalog \
-H "Authorization: Bearer $ACCESS_TOKEN" \
--json '{"catalogRole":{"name":"catalog_admin"}}'

# Give root the data engineer role
curl -X PUT http://localhost:8181/api/management/v1/principals/root/principal-roles \
-H "Authorization: Bearer $ACCESS_TOKEN" \
--json '{"principalRole": {"name":"data_engineer"}}'

# Verify if the role was correctly assigned to root.

curl -X GET http://localhost:8181/api/management/v1/principals/root/principal-roles -H "Authorization: Bearer $ACCESS_TOKEN" | jq

In order to run this application in your local it is recommended that we clone this in your local repo, use the YML file to start the docker services, once all the services are up and running you can execute spark streaming application in your local to consume the data.