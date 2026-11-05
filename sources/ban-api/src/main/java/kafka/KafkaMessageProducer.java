package kafka;

import config.BanApiConfig;
import lombok.extern.slf4j.Slf4j;
import model.BannedProduct;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import util.JsonSerializer;

import java.util.concurrent.ExecutionException;

/**
 * Продюсер запретов
 */
@Slf4j
public class KafkaMessageProducer {
    private static volatile KafkaMessageProducer kafkaMessageProducer;

    private final BanApiConfig config;
    private final Producer<String, String> producer;

    /**
     * Получить экземпляр продюсера
     */
    public static KafkaMessageProducer getInstance() {
        synchronized (KafkaMessageProducer.class) {
            if (kafkaMessageProducer == null) {
                kafkaMessageProducer = new KafkaMessageProducer();
            }
        }
        return kafkaMessageProducer;
    }

    private KafkaMessageProducer() {
        this.config = BanApiConfig.getInstance();

        this.producer = new KafkaProducer<>(this.config.toProducerProperties());
    }

    public static void closeProducer() {
        if (kafkaMessageProducer != null) {
            kafkaMessageProducer.producer.close();
            log.info("Продюсер остановлен");
        }
    }

    /**
     * Отправить запрет в топик kafka
     * @param bannedProduct запрет
     */
    public void sendBan(BannedProduct bannedProduct) throws ExecutionException, InterruptedException {
        String message = JsonSerializer.toJsonString(bannedProduct);

        ProducerRecord<String, String> record = new ProducerRecord<>(
                config.getBannedProductsTopicName(),
                bannedProduct.getProductId(),
                message);
        producer.send(record).get();
        log.info("Запрет доставлен в топик: {}", bannedProduct.getProductId());
    }

    /**
     * Снять запрет: tombstone-сообщение с ключом product_id
     * @param productId идентификатор товара
     */
    public void sendUnban(String productId) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                config.getBannedProductsTopicName(),
                productId,
                null);
        producer.send(record).get();
        log.info("Снятие запрета доставлено в топик: {}", productId);
    }
}
