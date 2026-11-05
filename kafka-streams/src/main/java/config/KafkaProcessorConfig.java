package config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.io.File;
import java.util.List;
import java.util.Properties;

/**
 * Конфигурация процессора
 */
@Slf4j
@Data
public class KafkaProcessorConfig {
    private static volatile KafkaProcessorConfig kafkaProcessorConfig;

    // Топик продуктов
    private final String productsTopicName;
    // Топик забаненных продуктов
    private final String bannedProductsTopicName;
    // Топик отфильтрованных продуктов
    private final String filteredProductsTopicName;

    // Имя хранилища забаненных продуктов
    private final String bannedProductsStore;

    // Сервера kafka
    private final String bootstrapServers;
    // Название сериализатора ключей
    private final String keySerializer;
    // Название сериализатора значений
    private final String valueSerializer;
    // Конфигурация подтверждения отправки
    private final String acks;
    // Количество попыток отправки сообщения
    private final String retries;
    // Адрес сервиса схемы
    private final String schemaRegistry;

    private final String securityProtocol;
    private final String sslEndpointIdentificationAlgorithm;
    private final String sslTruststoreLocation;
    private final String sslTruststorePassword;
    private final String sslKeystoreLocation;
    private final String sslKeystorePassword;

    private final String saslUsername;
    private final String saslPassword;

    public static KafkaProcessorConfig getInstance() {
        if (kafkaProcessorConfig == null) {
            kafkaProcessorConfig = new KafkaProcessorConfig();
        }
        return kafkaProcessorConfig;
    }

    private KafkaProcessorConfig() {
        Configurations configs = new Configurations();

        try {
            Configuration config = configs.properties(new File("application.properties"));

            this.productsTopicName = getEnvOrConfigValue(config, "topic.products.name", "KAFKA_PRODUCTS_TOPIC_NAME");
            this.bannedProductsTopicName = getEnvOrConfigValue(config, "topic.banned-products.name", "KAFKA_BANNED_PRODUCTS_TOPIC_NAME");
            this.filteredProductsTopicName = getEnvOrConfigValue(config, "topic.filtered-products.name", "KAFKA_FILTERED_PRODUCTS_TOPIC_NAME");

            this.bannedProductsStore = getEnvOrConfigValue(config, "store.banned-products.name", "KAFKA_BANNED_PRODUCTS_STORE");

            this.bootstrapServers = getEnvOrConfigValue(config, "bootstrap.servers", "KAFKA_BOOTSTRAP_SERVERS");
            this.schemaRegistry = getEnvOrConfigValue(config, "schema.registry", "KAFKA_SCHEMA_REGISTRY");

            this.keySerializer = config.getString("key.serializer");
            this.valueSerializer = config.getString("value.serializer");
            this.acks = config.getString("acks");
            this.retries = config.getString("retries");

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

    /**
     * Получить настройки в виде свойств потоков
     * @return Properties потоков
     */
    public Properties toStreamProperties() {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        if (this.acks != null) {
            props.put(ProducerConfig.ACKS_CONFIG, this.acks);
        }

        if (this.retries != null) {
            props.put(ProducerConfig.RETRIES_CONFIG, this.retries);
        }

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, this.securityProtocol);
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, this.sslTruststoreLocation); // Truststore
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.sslTruststorePassword); // Truststore password
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, this.sslKeystoreLocation); // Keystore
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.sslKeystorePassword); // Keystore password
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
