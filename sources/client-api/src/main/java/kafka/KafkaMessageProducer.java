package kafka;

import config.ClientApiConfig;
import lombok.extern.slf4j.Slf4j;
import model.ClientRequest;
import model.Product;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import util.JsonSerializer;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Класс просюсера
 */
@Slf4j
public class KafkaMessageProducer {
    private static volatile KafkaMessageProducer kafkaMessageProducer;

    private final ClientApiConfig config;
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
        this.config = ClientApiConfig.getInstance();

        this.producer = new KafkaProducer<>(this.config.toProducerProperties());
    }

    public static void closeProducer() {
        if (kafkaMessageProducer != null) {
            kafkaMessageProducer.producer.close();
            log.info("Продюсер остановлен");
        }
    }

    /**
     * Отправить сообщение в топик kafka
     * @param request запрос
     */
    public void sendRequest(ClientRequest request) throws ExecutionException, InterruptedException {
        String message = JsonSerializer.toJsonString(request);

        ProducerRecord<String, String> record = new ProducerRecord<>(
                config.getClientRequestsTopicName(),
                request.getUserId(),
                message);
        producer.send(record).get();
        log.info("Сообщение доставлено в топик");
    }

}
