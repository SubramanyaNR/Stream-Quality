# Keep tag in lock-step with flink.version in flink-job/pom.xml
ARG FLINK_IMAGE=flink:2.2.1-java17
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY flink-job/pom.xml .
RUN mvn -q -B dependency:go-offline
COPY flink-job/src ./src
RUN mvn -q -B -DskipTests package

FROM ${FLINK_IMAGE}
# The Prometheus reporter already ships in plugins/ (Flink 2.x). Make the shared
# checkpoint volume writable by the unprivileged flink user.
RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints
COPY --from=build /build/target/stream-quality-job.jar /opt/flink/usrlib/stream-quality-job.jar
COPY flink/bin/submit-job.sh /opt/flink/bin/submit-job.sh
USER root
RUN chmod 755 /opt/flink/bin/submit-job.sh
USER flink
