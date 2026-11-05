FROM openjdk:17.0.1-jdk-slim

COPY hdfs-consumer/target/hdfs-consumer.jar .

ENTRYPOINT ["java", "-jar", "hdfs-consumer.jar"]