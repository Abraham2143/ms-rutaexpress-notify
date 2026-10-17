FROM maven:3.9.16-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY src src
RUN mvn -B -ntp clean package

FROM eclipse-temurin:21-jre-jammy
RUN groupadd --gid 10001 notify && useradd --uid 10001 --gid notify --no-create-home notify \
    && mkdir -p /app/data && chown -R notify:notify /app
WORKDIR /app
COPY --from=build /workspace/target/ms-rutaexpress-notify-0.0.1-SNAPSHOT.jar app.jar
USER 10001:10001
ENV NOTIFY_STORAGE_PATH=/app/data
VOLUME ["/app/data"]
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
