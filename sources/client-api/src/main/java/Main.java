import kafka.KafkaAnalystConsumer;
import kafka.KafkaMessageProducer;
import lombok.extern.slf4j.Slf4j;
import service.ClientHttpServer;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class Main {
    public static void main(String[] args) {
        try {
            log.info("Инициализация HTTP сервера...");
            ClientHttpServer.getInstance().startServer();
            log.info("Сервер запущен!");
        } catch (Exception e) {
            log.error("Ошибка на сервере: {}", e.getMessage(), e);
            return;
        }

        AtomicBoolean running = new AtomicBoolean(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Выключение...");
            running.set(false);
            try {
                KafkaAnalystConsumer.getInstance().wakeup();
            } catch (Exception e) {
                log.warn("Не удалось разбудить consumer: {}", e.getMessage());
            }
            try {
                ClientHttpServer.stopServer();
            } catch (Exception e) {
                log.warn("Не удалось остановить HTTP-сервер: {}", e.getMessage());
            }
            try {
                KafkaMessageProducer.closeProducer();
            } catch (Exception e) {
                log.warn("Не удалось остановить producer: {}", e.getMessage());
            }
        }, "client-api-shutdown"));

        KafkaAnalystConsumer.getInstance().pollMessages(running);
    }
}
