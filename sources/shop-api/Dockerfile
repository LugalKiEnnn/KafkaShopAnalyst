#FROM maven:3.9.9-eclipse-temurin-17 AS builder
#
#WORKDIR /app
#
#COPY producer/pom.xml .
#
#RUN mvn dependency:go-offline
#COPY producer/src ./src
#
#RUN mvn clean package -DskipTests
#
#FROM openjdk:17.0.1-jdk-slim
#
#WORKDIR /app
#
#COPY --from=builder /app/target/producer.jar .
#
#ENTRYPOINT ["java", "-jar", "producer.jar"]

FROM openjdk:17.0.1-jdk-slim

COPY shop-api/target/shop-api.jar .

ENTRYPOINT ["java", "-jar", "shop-api.jar"]
