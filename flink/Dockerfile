# Keep tag in lock-step with flink.version in flink-job/pom.xml
ARG FLINK_IMAGE=flink:2.2.1-java17
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY flink-job/pom.xml .
RUN mvn -q -B dependency:go-offline
COPY flink-job/src ./src
RUN mvn -q -B -DskipTests package

FROM ${FLINK_IMAGE}
# Enable the Prometheus reporter (ships in opt/, must live in its own plugins/ subdir)
# and make the shared checkpoint volume writable by the unprivileged flink user.
RUN mkdir -p /opt/flink/plugins/metrics-prometheus /opt/flink/checkpoints && \
    cp /opt/flink/opt/flink-metrics-prometheus-*.jar /opt/flink/plugins/metrics-prometheus/ && \
    chown -R flink:flink /opt/flink/checkpoints
COPY --from=build /build/target/stream-quality-job.jar /opt/flink/usrlib/stream-quality-job.jar
COPY flink/bin/submit-job.sh /opt/flink/bin/submit-job.sh
RUN chmod +x /opt/flink/bin/submit-job.sh
