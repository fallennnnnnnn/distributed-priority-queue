# Build the service with the Gradle wrapper, so the image needs nothing installed but Docker.
FROM eclipse-temurin:17-jdk AS build
WORKDIR /src
COPY gradlew settings.gradle build.gradle ./
COPY gradle gradle
COPY src/main src/main
RUN ./gradlew --no-daemon bootJar

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/build/libs/*-SNAPSHOT.jar app.jar
# 8080: the REST API for clients. 9090: gRPC between brokers.
EXPOSE 8080 9090
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
