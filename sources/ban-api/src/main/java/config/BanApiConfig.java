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

import java.io.File;
import java.util.Properties;

/**
 * Конфигурация BAN API
 */
@Slf4j
@Data
public class BanApiConfig {
    private static volatile BanApiConfig banApiConfig;

    // Порт HTTP сервера
    private final Integer httpPort;

    // Топик запрещённых товаров
    private final String bannedProductsTopicName;

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

    private final String securityProtocol;
    private final String sslEndpointIdentificationAlgorithm;
    private final String sslTruststoreLocation;
    private final String sslTruststorePassword;
    private final String sslKeystoreLocation;
    private final String sslKeystorePassword;

    private final String saslUsername;
    private final String saslPassword;

    public static BanApiConfig getInstance() {
        if (banApiConfig == null) {
            synchronized (BanApiConfig.class) {
                banApiConfig = new BanApiConfig();
            }
        }
        return banApiConfig;
    }

    private BanApiConfig() {
        Configurations configs = new Configurations();

        try {
            Configuration config = configs.properties(new File("application.properties"));

            this.httpPort = Integer.valueOf(getEnvOrConfigValue(config, "http.port", "HTTP_PORT"));

            this.bannedProductsTopicName = getEnvOrConfigValue(config, "topic.banned-products.name", "KAFKA_BANNED_TOPIC_NAME");

            this.bootstrapServers = getEnvOrConfigValue(config, "bootstrap.servers", "KAFKA_BOOTSTRAP_SERVERS");
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

    public Properties toProducerProperties() {
        if (this.bootstrapServers == null || this.sslTruststoreLocation == null
                || this.sslTruststorePassword == null || this.sslKeystoreLocation == null
                || this.sslKeystorePassword == null || this.saslUsername == null || this.saslPassword == null) {
            throw new IllegalStateException("Не заданы параметры подключения к Kafka: проверьте KAFKA_* переменные окружения");
        }

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.valueSerializer);

        if (this.acks != null) {
            props.put(ProducerConfig.ACKS_CONFIG, this.acks);
        }

        if (this.retries != null) {
            props.put(ProducerConfig.RETRIES_CONFIG, this.retries);
        }

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
