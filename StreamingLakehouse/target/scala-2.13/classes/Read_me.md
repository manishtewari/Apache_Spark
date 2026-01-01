
# Create trino catalog iceberg properties file
mkdir -p ./trino/catalog/

cat << 'EOF' > ./trino/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris:8181/api/catalog/
iceberg.rest-catalog.warehouse=polariscatalog
iceberg.rest-catalog.vended-credentials-enabled=true
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=root:secret
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
# Required for Trino to read from/write to S3
fs.native-s3.enabled=true 
s3.endpoint=http://minio:9000 
s3.region=dummy-region
EOF

# Start the Docker Composer
docker compose up

# Post docker command we can access Trino, Minio in below URL
Trino Web UI: http://localhost:8080 

MinIO UI: http://localhost:9001 (admin/password) 

MinIO API: http://localhost:9000

Polaris: http://localhost:8181


# Steps to setup polaris catalog.
# Create access token 

ACCESS_TOKEN=$(curl -X POST 
http://localhost:8181/api/catalog/v1/oauth/tokens -d \
'grant_type=client_credentials&client_id=root&client_secret=secret&scope=PRINCIPAL_ROLE:ALL' | jq -r '.access_token')

# Use above access token while creating the catalog, which is created with various other details like storage location.

curl -i -X POST \
-H "Authorization: Bearer $ACCESS_TOKEN" \
http://localhost:8181/api/management/v1/catalogs \
--json '{
"name": "polariscatalog",
"type": "INTERNAL",
"properties": {
"default-base-location": "s3://warehouse",
"s3.endpoint": "http://minio:9000",
"s3.path-style-access": "true",
"s3.access-key-id": "admin",
"s3.secret-access-key": "password",
"s3.region": "dummy-region"
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
