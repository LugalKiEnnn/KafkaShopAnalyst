FROM confluentinc/cp-kafka-connect:7.7.1

# Установка библиотеки
COPY confluentinc-kafka-connect-elasticsearch-14.1.0.zip /tmp/
RUN confluent-hub install --no-prompt \
    /tmp/confluentinc-kafka-connect-elasticsearch-14.1.0.zip

# Скопировать JMX Exporter в докер образ
COPY jmx_prometheus_javaagent-0.15.0.jar /opt/
# Скопировать настройки JMX Exporter в докер образ
COPY kafka-connect.yml /opt/
#COPY debezium-connect.yml /opt/
