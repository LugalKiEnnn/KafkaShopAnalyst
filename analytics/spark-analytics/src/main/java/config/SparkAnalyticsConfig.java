package config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Data
public class SparkAnalyticsConfig {
    private static volatile SparkAnalyticsConfig sparkAnalyticsConfig;

    private final String analystTopicName;

    private final String hdfsUri;
    private final String hdfsUser;
    private final String hdfsRawPath;
    private final String hdfsCheckpointPath;

    private final String bootstrapServers;

    private final String securityProtocol;
    private final String sslEndpointIdentificationAlgorithm;
    private final String sslTruststoreLocation;
    private final String sslTruststorePassword;
    private final String sslKeystoreLocation;
    private final String sslKeystorePassword;
    private final String sslKeyPassword;

    private final String saslUsername;
    private final String saslPassword;

    public static SparkAnalyticsConfig getInstance() {
        synchronized (SparkAnalyticsConfig.class) {
            if (sparkAnalyticsConfig == null) {
                sparkAnalyticsConfig = new SparkAnalyticsConfig();
            }
        }

        return sparkAnalyticsConfig;
    }

    private SparkAnalyticsConfig() {
        Properties properties = loadProperties();

        this.analystTopicName = getEnvOrProperty(properties, "topic.analyst.name", "ANALYST_TOPIC_NAME");

        this.hdfsUri = getEnvOrProperty(properties, "hdfs.uri", "HDFS_URI");
        this.hdfsUser = getEnvOrProperty(properties, "hdfs.user", "HDFS_USER");
        String rawPath = getEnvOrProperty(properties, "hdfs.raw.path", "HDFS_RAW_PATH");
        this.hdfsRawPath = rawPath != null ? rawPath : "/data/raw";

        String checkpointPath = getEnvOrProperty(properties, "hdfs.checkpoint.path", "HDFS_CHECKPOINT_PATH");
        this.hdfsCheckpointPath = checkpointPath != null ? checkpointPath : "/data/checkpoints/recommendations";

        this.bootstrapServers = getEnvOrProperty(properties, "bootstrap.servers", "KAFKA_BOOTSTRAP_SERVERS");

        this.securityProtocol = getEnvOrProperty(properties, "security.protocol", "KAFKA_SECURITY_PROTOCOL");

        String endpointAlgorithm = getEnvOrProperty(
                properties, "ssl.endpoint.identification.algorithm",
                "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM");
        this.sslEndpointIdentificationAlgorithm = endpointAlgorithm != null ? endpointAlgorithm : "https";

        this.sslTruststoreLocation = getEnvOrProperty(properties, "ssl.truststore.location", "KAFKA_SSL_TRUSTSTORE_LOCATION");
        this.sslTruststorePassword = getEnvOrProperty(properties, "ssl.truststore.password", "KAFKA_SSL_TRUSTSTORE_PASSWORD");
        this.sslKeystoreLocation = getEnvOrProperty(properties, "ssl.keystore.location", "KAFKA_SSL_KEYSTORE_LOCATION");
        this.sslKeystorePassword = getEnvOrProperty(properties, "ssl.keystore.password", "KAFKA_SSL_KEYSTORE_PASSWORD");

        String keyPassword = getEnvOrProperty(properties, "ssl.key.password", "KAFKA_SSL_KEY_PASSWORD");
        this.sslKeyPassword = keyPassword != null ? keyPassword : this.sslKeystorePassword;

        this.saslUsername = getEnvOrProperty(properties, "sasl.username", "KAFKA_SASL_USERNAME");
        this.saslPassword = getEnvOrProperty(properties, "sasl.password", "KAFKA_SASL_PASSWORD");
    }

    public Map<String, String> toKafkaSinkOptions() {
        if (this.analystTopicName == null || this.bootstrapServers == null
                || this.securityProtocol == null || this.sslTruststoreLocation == null
                || this.sslTruststorePassword == null || this.sslKeystoreLocation == null
                || this.sslKeystorePassword == null || this.saslUsername == null
                || this.saslPassword == null) {
            throw new IllegalStateException(
                    "Не заданы параметры Kafka: проверьте KAFKA_* переменные окружения");
        }
        if (this.hdfsUri == null || this.hdfsUser == null || this.hdfsRawPath == null
                || this.hdfsCheckpointPath == null) {
            throw new IllegalStateException("Не заданы параметры HDFS: проверьте HDFS_URI/HDFS_USER/HDFS_RAW_PATH/HDFS_CHECKPOINT_PATH");
        }

        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required"
                + " username=\"" + this.saslUsername + "\""
                + " password=\"" + this.saslPassword + "\";";

        Map<String, String> options = new HashMap<>();
        options.put("kafka.bootstrap.servers", this.bootstrapServers);
        options.put("kafka.security.protocol", this.securityProtocol);
        options.put("kafka.sasl.mechanism", "SCRAM-SHA-512");
        options.put("kafka.sasl.jaas.config", jaasConfig);
        options.put("kafka.ssl.truststore.location", this.sslTruststoreLocation);
        options.put("kafka.ssl.truststore.password", this.sslTruststorePassword);
        options.put("kafka.ssl.keystore.location", this.sslKeystoreLocation);
        options.put("kafka.ssl.keystore.password", this.sslKeystorePassword);
        options.put("kafka.ssl.key.password", this.sslKeyPassword);
        options.put("kafka.ssl.endpoint.identification.algorithm", this.sslEndpointIdentificationAlgorithm);
        return options;
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();

        try (InputStream in = SparkAnalyticsConfig.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (in != null) {
                properties.load(in);
                return properties;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Не удалось прочитать application.properties из classpath", e);
        }

        Path file = Path.of("application.properties");
        if (Files.exists(file)) {
            try (InputStream in = Files.newInputStream(file)) {
                properties.load(in);
                return properties;
            } catch (IOException e) {
                throw new IllegalStateException("Не удалось прочитать " + file, e);
            }
        }

        throw new IllegalStateException("Файл application.properties не найден");
    }

    /**
     * Поиск значения конфигурации либо в env переменных, либо в application.properties.
     */
    private String getEnvOrProperty(Properties properties, String key, String envVar) {
        String envValue = System.getenv(envVar);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        String fileValue = properties.getProperty(key);
        if (fileValue != null && !fileValue.isEmpty() && !fileValue.startsWith("${")) {
            return fileValue;
        }
        return null;
    }
}
