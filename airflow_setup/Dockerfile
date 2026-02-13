FROM apache/airflow:2.10.2

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN mkdir -p /opt/airflow/lib

COPY airflow/jars.conf /opt/airflow/jars.conf
COPY airflow/download_jars.sh /opt/airflow/download_jars.sh

RUN sed -i 's/\r$//' /opt/airflow/download_jars.sh && \
    chmod +x /opt/airflow/download_jars.sh && \
    /opt/airflow/download_jars.sh && \
    find /opt/airflow/lib -name "*.jar" -exec chmod 644 {} +

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt"

RUN pip install --no-cache-dir "pyspark==3.5.2"
