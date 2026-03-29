FROM apache/airflow:2.8.1

USER root

# Install Java (Required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget && \
    apt-get clean

# Set Java home globally
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

# Install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set Spark Home
ENV SPARK_HOME=/opt/spark

# Update PATH to include Airflow's local bin, Spark, and Java
ENV PATH="/home/airflow/.local/bin:/opt/spark/bin:${JAVA_HOME}/bin:${PATH}"

# Switch to airflow user for python packages
USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-google \
    apache-airflow-providers-apache-spark \
    pyspark==3.5.0 \
    pandas \
    google-cloud-storage \
    google-cloud-bigquery \
    kaggle