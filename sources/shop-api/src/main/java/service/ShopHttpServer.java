package service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import config.ShopApiConfig;
import kafka.KafkaMessageProducer;
import lombok.extern.slf4j.Slf4j;
import model.Product;
import util.JsonDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * HTTP сервер SHOP API
 */
@Slf4j
public class ShopHttpServer {
    private static volatile ShopHttpServer shopHttpServer;
    private final HttpServer httpServer;

    /**
     * Получить экземпляр http сервера
     */
    public static ShopHttpServer getInstance() throws IOException {
        if (shopHttpServer == null) {
            synchronized (ShopHttpServer.class) {
                shopHttpServer = new ShopHttpServer();
            }
        }
        return shopHttpServer;
    }

    private ShopHttpServer() throws IOException {
        this.httpServer = HttpServer.create(new InetSocketAddress(ShopApiConfig.getInstance().getHttpPort()), 0);

        createContext();

        this.httpServer.setExecutor(null);
    }

    /**
     * Создание HTTP методов
     */
    private void createContext() {
        // Проверка доступности сервера
        this.httpServer.createContext("/api/health", (exchange -> {
            if ("GET".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 200, "Ok");
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
            exchange.close();
        }));

        // Загрузка всех товаров из файла
        this.httpServer.createContext("/api/products/load", (exchange -> {
            if ("POST".equals(exchange.getRequestMethod())) {
                try {
                    List<Product> products = ProductLoader.loadProducts(ShopApiConfig.getInstance().getProductsFile());

                    for (Product product : products) {
                        log.info("Товар: {} {}", product.getProductId(), product.getName());

                        KafkaMessageProducer.getInstance().sendProduct(product);
                    }

                    sendResponse(exchange, 200, "Прочитано товаров: " + products.size());
                } catch (Exception e) {
                    sendResponse(exchange, 500, "Ошибка при загрузке товаров: " + e.getMessage());
                }
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
            exchange.close();
        }));

        // Отправка одного товара из тела запроса
        this.httpServer.createContext("/api/products/send", (exchange -> {
            if ("POST".equals(exchange.getRequestMethod())) {
                try (InputStream is = exchange.getRequestBody()) {
                    Product product = JsonDeserializer.fromBytesToObject(is.readAllBytes(), Product.class);

                    log.info("Товар: {} {}", product.getProductId(), product.getName());
                    KafkaMessageProducer.getInstance().sendProduct(product);

                    sendResponse(exchange, 200, "Товар принят: " + product.getProductId());
                } catch (Exception e) {
                    sendResponse(exchange, 500, "Ошибка при отправке товара: " + e.getMessage());
                }
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
            exchange.close();
        }));
    }

    private static void sendResponse(HttpExchange exchange, int code, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
        exchange.sendResponseHeaders(code, bytes.length);
        OutputStream output = exchange.getResponseBody();
        output.write(bytes);
        output.flush();
    }

    /**
     * Запуск сервера
     */
    public void startServer() {
        this.httpServer.start();
    }

    /**
     * Остановка сервера
     */
    public static void stopServer() {
        if (shopHttpServer != null) {
            shopHttpServer.httpServer.stop(5);
            log.info("Сервер остановлен");
        }
    }
}
