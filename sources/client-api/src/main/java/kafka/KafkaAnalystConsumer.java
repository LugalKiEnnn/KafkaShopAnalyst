package kafka;

import config.ClientApiConfig;
import lombok.extern.slf4j.Slf4j;
import model.ClientRequest;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaAnalystConsumer {
    private static volatile KafkaAnalystConsumer kafkaAnalystConsumer;

    private final ClientApiConfig config;
    private final Consumer<String, String> consumer;
    private final String consumerGroupId;

    private final Map<String, String> userRecommendationMap = new ConcurrentHashMap<>();

    public static KafkaAnalystConsumer getInstance() {
        synchronized (KafkaAnalystConsumer.class) {
            if (kafkaAnalystConsumer == null) {
                kafkaAnalystConsumer = new KafkaAnalystConsumer();
            }
        }

        return kafkaAnalystConsumer;
    }

    private KafkaAnalystConsumer() {
        this.config = ClientApiConfig.getInstance();

        String baseGroup = this.config.getGroupId();
        if (baseGroup == null || baseGroup.isBlank()) {
            baseGroup = "client-api-group";
        }
        this.consumerGroupId = baseGroup + "-" + UUID.randomUUID();

        Properties properties = this.config.toConsumerProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, this.consumerGroupId);

        this.consumer = new KafkaConsumer<>(properties);
        this.consumer.subscribe(Collections.singletonList(config.getAnalyticsTopicName()));
    }

    /**
     * Читать рекомендации из второго кластера, пока приложение работает.
     * Ошибки poll не завершают consumer: логируем и повторяем с паузой.
     */
    public void pollMessages(AtomicBoolean running) {
        try {
            log.info("Consumer запущен: topic={}, group={}",
                    config.getAnalyticsTopicName(), consumerGroupId);

            while (running.get()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        if (record.key() == null || record.value() == null) {
                            log.warn("Пропущена рекомендация без ключа или значения: key={}, value={}",
                                    record.key(), record.value());
                            continue;
                        }

                        log.info("Получена рекомендация: userId = {}, value = {}", record.key(), record.value());
                        userRecommendationMap.put(record.key(), record.value());
                    }
                } catch (WakeupException e) {
                    if (!running.get()) {
                        break;
                    }
                    log.warn("Неожиданный wakeup consumer: {}", e.getMessage());
                } catch (Exception e) {
                    log.error("Ошибка consumer: {}", e.getMessage(), e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            try {
                this.consumer.close();
            } catch (Exception e) {
                log.warn("Ошибка закрытия consumer: {}", e.getMessage());
            }
            log.info("Consumer остановлен");
        }
    }

    public String getRecommendationByRequest(ClientRequest request) {
        if (request == null) {
            return null;
        }

        return getUserRecommendation(request.getUserId());
    }

    public String getUserRecommendation(String userId) {
        if (userId == null || userId.isBlank()) {
            return null;
        }

        return userRecommendationMap.get(userId);
    }

    public void wakeup() {
        this.consumer.wakeup();
    }
}
