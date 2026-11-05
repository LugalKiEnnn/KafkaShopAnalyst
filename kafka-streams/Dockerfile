FROM openjdk:17.0.1-jdk-slim

COPY kafka-streams/target/kafka-streams.jar .

ENTRYPOINT ["java", "-jar", "kafka-streams.jar"]