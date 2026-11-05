import kafka.KafkaMessageProducer;
import lombok.extern.slf4j.Slf4j;
import service.BanHttpServer;

@Slf4j
public class Main {
    public static void main(String[] args) {
        try {
            log.info("Инициализация HTTP сервера...");
            BanHttpServer.getInstance().startServer();
            log.info("Сервер запущен!");
        } catch (Exception e) {
            log.error("Ошибка на сервере: {}", e.getMessage());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Выключение...");
            BanHttpServer.stopServer();
            KafkaMessageProducer.closeProducer();
        }));
    }
}
