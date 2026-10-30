# syntax=docker/dockerfile:1

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# Optional Maven Central mirror, e.g. when repo.maven.apache.org returns 429.
ARG MAVEN_MIRROR=""
RUN if [ -n "$MAVEN_MIRROR" ]; then \
      mkdir -p /root/.m2 && printf '<settings><mirrors><mirror><id>central-mirror</id><mirrorOf>central</mirrorOf><url>%s</url></mirror></mirrors></settings>\n' "$MAVEN_MIRROR" > /root/.m2/settings.xml; \
    fi

COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

RUN useradd --system --uid 10001 --home-dir /app icu
COPY --from=build /workspace/target/server-0.0.1-SNAPSHOT.jar /app/server.jar
RUN mkdir -p /app/logs && chown -R icu:icu /app

USER icu
EXPOSE 8080

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/server.jar"]
