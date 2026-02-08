import socket
import requests
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import socket
from airflow.operators.python import PythonOperator

# Constants
POLARIS_URI = "http://polaris:8181/api/catalog/v1/oauth/tokens"
CLIENT_ID = "root"
CLIENT_SECRET = "secret"
CATALOG_NAME = "warehouse"

def get_driver_host_ip():
    """Dynamically resolves the internal Docker IP of the current container."""
    try:
        host_name = socket.gethostname()
        return socket.gethostbyname(host_name)
    except Exception as e:
        print(f"Could not resolve dynamic IP: {e}")
        # Fallback to a common Docker gateway IP if resolution fails
        return "172.18.0.1"


# Call it here so it's available to the DAG
DYNAMIC_DRIVER_IP = get_driver_host_ip()

def test_infrastructure_readiness():
    """Verifies all external service connections before starting Spark."""
    services = {
        "Polaris API": ("polaris", 8181),
        "MinIO API": ("minio", 9000),
        "Spark Master": ("spark-master", 7077)
    }

    # 1. Physical Port Check (Socket level)
    for name, (host, port) in services.items():
        try:
            with socket.create_connection((host, port), timeout=5):
                print(f" {name} reachable at {host}:{port}")
        except (socket.timeout, ConnectionRefusedError):
            raise RuntimeError(f" {name} is UNREACHABLE at {host}:{port}. Check Docker network.")

    # 2. Functional Polaris Auth Check
    payload = {
        'grant_type': 'client_credentials',
        'scope': 'PRINCIPAL_ROLE:ALL',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }
    response = requests.post(POLARIS_URI, data=payload, timeout=10)
    response.raise_for_status()
    print(" Successfully authenticated with Polaris!")

with DAG(
    dag_id='spark_lakehouse_lean_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['iceberg', 'polaris', 'minio']
) as dag:

    # The new robust check
    infra_check = PythonOperator(
        task_id='pre_flight_check',
        python_callable=test_infrastructure_readiness
    )

    execute_lakehouse_batch = SparkSubmitOperator(
        task_id='execute_lakehouse_batch',
        conn_id='spark_default',
        application='/opt/airflow/lib/batchlakehouse_2.12-0.1.jar',
        java_class='com.spark.app.NycTaxiDataAnalysis',
        jars=(
            "/opt/airflow/lib/iceberg-spark-runtime-3.5_2.12-1.10.0.jar,"
            "/opt/airflow/lib/iceberg-aws-bundle-1.10.0.jar,"
            "/opt/airflow/lib/hadoop-aws-3.3.4.jar,"
            "/opt/airflow/lib/aws-java-sdk-bundle-1.12.262.jar,"
            "/opt/airflow/lib/config-1.4.2.jar,"
            "/opt/airflow/lib/okhttp-4.12.0.jar,"
            "/opt/airflow/lib/minio-8.6.0.jar,"
            "/opt/airflow/lib/okio-jvm-3.9.0.jar,"
            "/opt/airflow/lib/kotlin-stdlib-1.9.10.jar,"
            "/opt/airflow/lib/guava-33.0.0-jre.jar,"    # Your new JAR
            "/opt/airflow/lib/failureaccess-1.0.2.jar,"
            "/opt/airflow/lib/simple-xml-2.7.1.jar"
        ),
        application_args=[
            "https://d37ci6vzurychx.cloudfront.net/",
            "misc,taxi_zone_lookup.csv|data,yellow_tripdata_2025-01.parquet",
            "true"
        ],
        conf={
            # --- Networking (Crucial for Docker/Airflow) ---
            "spark.driver.host": DYNAMIC_DRIVER_IP,
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.master": "spark://spark-master:7077",
            "spark.driver.userClassPathFirst": "true",
            "spark.executor.userClassPathFirst": "true",
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.memory.offHeap.enabled": "true",
             "spark.memory.offHeap.size": "512m",
             "spark.driver.maxResultSize": "1g",
             "spark.sql.catalog.warehouse.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
            "spark.driver.extraJavaOptions": "-Dconfig.file=/opt/airflow/lib/batch.conf",
            "spark.executor.extraJavaOptions": "-Dconfig.file=/opt/airflow/lib/batch.conf"

        }
    )
    execute_lakehouse_streaming = SparkSubmitOperator(
        task_id='execute_lakehouse_streaming',
        conn_id='spark_default',
        application='/opt/airflow/lib/sparkstructuredstreaming_2.12-0.1.jar',
        java_class='com.spark.appStreaming.StreamingTransactions',
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.apache.hadoop:hadoop-aws:3.3.4",
        jars=(
            "/opt/airflow/lib/iceberg-spark-runtime-3.5_2.12-1.10.0.jar,"
            "/opt/airflow/lib/iceberg-aws-bundle-1.10.0.jar,"
            "/opt/airflow/lib/config-1.4.2.jar,"
            "/opt/airflow/lib/okhttp-4.12.0.jar,"
            "/opt/airflow/lib/minio-8.6.0.jar,"
            "/opt/airflow/lib/okio-jvm-3.9.0.jar,"
            "/opt/airflow/lib/simple-xml-2.7.1.jar"
        ),
        conf={
            # --- Networking (Crucial for Docker/Airflow) ---
            "spark.driver.host": DYNAMIC_DRIVER_IP,
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.master": "spark://spark-master:7077",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.driver.userClassPathFirst": "false",
            "spark.executor.userClassPathFirst": "false",
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.memory.offHeap.enabled": "true",
             "spark.memory.offHeap.size": "512m",
             "spark.driver.maxResultSize": "1g",
             "spark.sql.catalog.warehouse.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
            "spark.driver.extraJavaOptions": "-Dconfig.file=/opt/airflow/lib/streaming.conf",
            "spark.executor.extraJavaOptions": "-Dconfig.file=/opt/airflow/lib/streaming.conf"
             }
    )

    infra_check >>  [execute_lakehouse_streaming , execute_lakehouse_batch]