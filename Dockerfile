FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-fetch Delta Lake jars into Ivy cache
RUN python3 -c "from pyspark.sql import SparkSession; \
    SparkSession.builder.master('local[1]') \
    .config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.2.0') \
    .getOrCreate().stop()"

COPY . /app/

CMD ["python", "pipeline/stream_engine.py"]
