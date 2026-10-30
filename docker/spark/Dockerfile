FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu@sha256:9b0a6c2c860f5e7d18dd5270286fef32a09c7f5a7e2b0dbe5642de8a3a02ab3e

USER root

# The image ships Python 3.10, whose datetime.fromisoformat rejects the "Z"
# suffix used by every RAW event_time. PyFlink runs Python 3.12, so the shared
# jobs/flink logic gets the same minor version here (driver and workers).
ARG PYTHON_BUILD=20261013
ARG PYTHON_VERSION=3.12.15
ARG PYTHON_SHA256=5d546fc004fc2435fa142eb8f42ed8de5eb4725963dc22f7e6671014a80eb560
RUN curl --fail --location --retry 3 \
      "https://github.com/astral-sh/python-build-standalone/releases/download/${PYTHON_BUILD}/cpython-${PYTHON_VERSION}+${PYTHON_BUILD}-x86_64-unknown-linux-gnu-install_only.tar.gz" \
      -o /tmp/python.tar.gz \
    && echo "${PYTHON_SHA256}  /tmp/python.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/python.tar.gz -C /opt \
    && rm /tmp/python.tar.gz \
    && /opt/python/bin/python3 -m pip install --no-cache-dir \
       pandas==2.3.3 pyarrow==22.0.0 numpy==2.3.5 \
    && /opt/python/bin/python3 -m pip check

# Kafka source/sink for Structured Streaming plus its transitive runtime jars
# that the base image does not already contain.
ARG MAVEN_MIRROR=https://repo.maven.apache.org/maven2
ARG SPARK_VERSION=4.1.3
ARG KAFKA_CLIENTS_VERSION=3.9.1
ARG COMMONS_POOL2_VERSION=2.12.1
RUN set -e; \
    fetch() { curl --fail --location --retry 3 "${MAVEN_MIRROR}/$1" -o "/opt/spark/jars/$(basename "$1")"; \
              echo "$2  /opt/spark/jars/$(basename "$1")" | sha256sum -c -; }; \
    fetch org/apache/spark/spark-sql-kafka-0-10_2.13/${SPARK_VERSION}/spark-sql-kafka-0-10_2.13-${SPARK_VERSION}.jar \
          cb31ffd9c8f5527dba061db8af671d295562092e4f3f5ff42fc249fab8a8fde6; \
    fetch org/apache/spark/spark-token-provider-kafka-0-10_2.13/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.13-${SPARK_VERSION}.jar \
          0535a111d7d11b2859c0a0548d5c8d172c6925f9b8d041d0f0dd38ddc5a3497c; \
    fetch org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
          7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9; \
    fetch org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar \
          5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef

RUN mkdir -p /opt/icu/jobs /opt/icu/config /opt/icu/checkpoints /opt/icu/metrics \
    && chown -R spark:spark /opt/icu/checkpoints /opt/icu/metrics

# jobs/flink is mounted read-only at runtime, exactly like the Flink services.
COPY jobs/spark/ /opt/icu/jobs/spark/

ENV PATH="/opt/python/bin:/opt/spark/bin:${PATH}" \
    PYSPARK_PYTHON=/opt/python/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/opt/python/bin/python3 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip:/opt/icu/jobs/flink:/opt/icu/jobs/spark \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER spark
WORKDIR /opt/icu
