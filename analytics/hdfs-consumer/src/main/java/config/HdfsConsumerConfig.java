package config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.io.File;
import java.util.Properties;

/**
 * Конфигурация BAN API
 */
@Slf4j
@Data
public class HdfsConsumerConfig {
    private static volatile HdfsConsumerConfig hdfsConsumerConfig;

    // Топик отфильтрованных товаров
    private final String filteredProductsTopicName;
    private final String clientQueryTopicName;

    private final String groupId;

    private final String hdfsUri;
    private final String hdfsUser;
    private final String hdfsRawPath;

    // Сервера kafka
    private final String bootstrapServers;
    // Название десериализатора ключей
    private final String keyDeserializer;
    // Название десериализатора значений
    private final String valueDeserializer;

    private final String securityProtocol;
    private final String sslEndpointIdentificationAlgorithm;
    private final String sslTruststoreLocation;
    private final String sslTruststorePassword;
    private final String sslKeystoreLocation;
    private final String sslKeystorePassword;

    private final String saslUsername;
    private final String saslPassword;

    public static HdfsConsumerConfig getInstance() {
        synchronized (HdfsConsumerConfig.class) {
            if (hdfsConsumerConfig == null) {
                hdfsConsumerConfig = new HdfsConsumerConfig();
            }
        }

        return hdfsConsumerConfig;
    }

    private HdfsConsumerConfig() {
        Configurations configs = new Configurations();

        try {
            Configuration config = configs.properties(new File("application.properties"));

            this.filteredProductsTopicName = getEnvOrConfigValue(config, "topic.filtered-products.name", "FILTERED_TOPIC_NAME");
            this.clientQueryTopicName = getEnvOrConfigValue(config, "topic.client-query.name", "CLIENT_TOPIC_NAME");

            this.groupId = getEnvOrConfigValue(config, "group.id", "KAFKA_GROUP_ID");

            this.hdfsUri = getEnvOrConfigValue(config, "hdfs.uri", "HDFS_URI");
            this.hdfsUser = getEnvOrConfigValue(config, "hdfs.user", "HDFS_USER");
            String rawPath = getEnvOrConfigValue(config, "hdfs.raw.path", "HDFS_RAW_PATH");
            this.hdfsRawPath = rawPath != null ? rawPath : "/data/raw";

            this.bootstrapServers = getEnvOrConfigValue(config, "bootstrap.servers", "KAFKA_BOOTSTRAP_SERVERS");
            this.keyDeserializer = config.getString("key.deserializer");
            this.valueDeserializer = config.getString("value.deserializer");

            this.securityProtocol = getEnvOrConfigValue(config, "security.protocol", "KAFKA_SECURITY_PROTOCOL");
            this.sslEndpointIdentificationAlgorithm = config.getString("ssl.endpoint.identification.algorithm");
            this.sslTruststoreLocation = getEnvOrConfigValue(config, "ssl.truststore.location", "KAFKA_SSL_TRUSTSTORE_LOCATION");
            this.sslTruststorePassword = getEnvOrConfigValue(config, "ssl.truststore.password", "KAFKA_SSL_TRUSTSTORE_PASSWORD");
            this.sslKeystoreLocation = getEnvOrConfigValue(config, "ssl.keystore.location", "KAFKA_SSL_KEYSTORE_LOCATION");
            this.sslKeystorePassword = getEnvOrConfigValue(config, "ssl.keystore.password", "KAFKA_SSL_KEYSTORE_PASSWORD");

            this.saslUsername = getEnvOrConfigValue(config, "sasl.username", "KAFKA_SASL_USERNAME");
            this.saslPassword = getEnvOrConfigValue(config, "sasl.password", "KAFKA_SASL_PASSWORD");
        } catch (ConfigurationException cex) {
            log.error("Ошибка загрузки конфигурации: {}", cex.getMessage());

            throw new IllegalStateException("Ошибка загрузки конфигурации", cex.getCause());
        }
    }

    public Properties toConsumerProperties() {
        if (this.bootstrapServers == null || this.sslTruststoreLocation == null
                || this.sslTruststorePassword == null || this.sslKeystoreLocation == null
                || this.sslKeystorePassword == null || this.saslUsername == null || this.saslPassword == null) {
            throw new IllegalStateException("Не заданы параметры подключения к Kafka: проверьте KAFKA_* переменные окружения");
        }

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.valueDeserializer);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, this.securityProtocol);
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, this.sslTruststoreLocation);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.sslTruststorePassword);
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, this.sslKeystoreLocation);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.sslKeystorePassword);
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, this.sslEndpointIdentificationAlgorithm);

        String jaasConfigStr = "org.apache.kafka.common.security.scram.ScramLoginModule required username=" + "\"" + this.saslUsername + "\"" +
                " password=" + "\"" + this.saslPassword + "\";";
        props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfigStr);

        return props;
    }

    /**
     * Поиск значения конфигурации либо в env переменных, либо в application.properties
     * @param config конфигурация
     * @param key ключ поиска в application.properties
     * @param envVar наименование переменной в env
     * @return найденное значение
     */
    private String getEnvOrConfigValue(Configuration config, String key, String envVar) {
        String envValue = System.getenv(envVar);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        String fileValue = config.getString(key, null);
        if (fileValue != null && !fileValue.isEmpty() && !fileValue.startsWith("${")) {
            return fileValue;
        }
        return null;
    }
}
