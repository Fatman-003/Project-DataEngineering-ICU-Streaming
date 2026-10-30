FROM flink:2.3.0@sha256:df6b153c93d3e60f4b96b323979bf0305861949a600fcee287214609d325037e AS runtime-base

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-venv ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/pyflink \
    && /opt/pyflink/bin/pip install --no-cache-dir apache-flink==2.3.0 \
    && /opt/pyflink/bin/pip check
ENV PATH="/opt/pyflink/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Released for Flink 2.2; this exact binary is integration-tested on 2.3.0.
# Apache has not published an official 2.3 connector artifact yet.
ARG KAFKA_CONNECTOR_VERSION=5.0.0-2.2
ARG KAFKA_CONNECTOR_SHA256=5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616
ARG MAVEN_MIRROR=https://repo.maven.apache.org/maven2
RUN curl --fail --location --retry 3 \
      "${MAVEN_MIRROR}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
      -o /opt/flink/lib/flink-sql-connector-kafka.jar \
    && echo "${KAFKA_CONNECTOR_SHA256}  /opt/flink/lib/flink-sql-connector-kafka.jar" | sha256sum -c - \
    && cp /opt/flink/opt/flink-python-2.3.0.jar /opt/flink/lib/ \
    && mkdir -p /opt/flink/checkpoints \
    && chown flink:flink /opt/flink/checkpoints
USER flink


# Compile the narrow metadata/key bridge against the exact runtime connector.
# The build-stage JDK is excluded from the final Flink runtime image.
FROM runtime-base AS metadata-builder
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*
COPY java/ /tmp/icu-kafka-java/
RUN mkdir -p /tmp/icu-kafka-classes \
    && /usr/lib/jvm/java-17-openjdk-amd64/bin/javac --release 17 \
       -cp '/opt/flink/lib/*' -d /tmp/icu-kafka-classes \
       /tmp/icu-kafka-java/org/icu/streaming/*.java \
    && /usr/lib/jvm/java-17-openjdk-amd64/bin/jar --create \
       --file /tmp/icu-kafka-metadata.jar -C /tmp/icu-kafka-classes .

FROM runtime-base
COPY --from=metadata-builder --chown=flink:flink \
    /tmp/icu-kafka-metadata.jar /opt/flink/lib/icu-kafka-metadata.jar
