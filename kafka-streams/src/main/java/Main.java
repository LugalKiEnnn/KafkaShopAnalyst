import kafka.KafkaStreamProcessor;

import java.util.concurrent.CountDownLatch;

public class Main {
    public static void main(String[] args) {
        try {
            KafkaStreamProcessor.getInstance();

            final CountDownLatch latch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                KafkaStreamProcessor.closeProcessor();
                latch.countDown();
            }));
            latch.await();
        } catch (InterruptedException | RuntimeException e) {
            System.err.println("Ошибка при запуске приложения: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
