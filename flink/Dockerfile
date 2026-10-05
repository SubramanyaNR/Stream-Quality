# Keep tag in lock-step with flink.version in flink-job/pom.xml
ARG FLINK_IMAGE=flink:2.2.1-java17
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY flink-job/pom.xml .
RUN mvn -q -B dependency:go-offline
COPY flink-job/src ./src
RUN mvn -q -B -DskipTests package

FROM ${FLINK_IMAGE}
# Prometheus reporter ships in /opt/flink/plugins-available? -> enable it
RUN mkdir -p /opt/flink/plugins/metrics-prometheus && \
    cp /opt/flink/opt/flink-metrics-prometheus-*.jar /opt/flink/plugins/metrics-prometheus/
COPY --from=build /build/target/stream-quality-job.jar /opt/flink/usrlib/stream-quality-job.jar
COPY flink/bin/submit-job.sh /opt/flink/bin/submit-job.sh
RUN chmod +x /opt/flink/bin/submit-job.sh
