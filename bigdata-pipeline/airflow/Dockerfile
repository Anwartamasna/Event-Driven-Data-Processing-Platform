FROM apache/airflow:2.8.0

USER root

# Install OpenJDK-17 and other utilities
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install pyspark and apache-airflow-providers-apache-spark if needed
# (Though we might often use BashOperator to run scripts directly, installing pyspark ensures spark-submit is available if we use it via pip, 
#  but for a real "spark-submit" to a remote cluster, we usually download the spark binary. 
#  However, for this simulation, pip install pyspark includes the spark binaries which is sufficient for submitting jobs.)
RUN pip install --no-cache-dir pyspark==3.5.0 apache-airflow-providers-apache-spark kafka-python
