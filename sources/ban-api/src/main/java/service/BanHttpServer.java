package service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import config.BanApiConfig;
import kafka.KafkaMessageProducer;
import lombok.extern.slf4j.Slf4j;
import model.BannedProduct;
import util.JsonDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * HTTP сервер BAN API
 */
@Slf4j
public class BanHttpServer {
    private static volatile BanHttpServer banHttpServer;
    private final HttpServer httpServer;

    /**
     * Получить экземпляр http сервера
     */
    public static BanHttpServer getInstance() throws IOException {
        if (banHttpServer == null) {
            synchronized (BanHttpServer.class) {
                banHttpServer = new BanHttpServer();
            }
        }
        return banHttpServer;
    }

    private BanHttpServer() throws IOException {
        this.httpServer = HttpServer.create(new InetSocketAddress(BanApiConfig.getInstance().getHttpPort()), 0);

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

        // Добавление запрета (POST) и снятие запрета (DELETE /api/bans/{product_id})
        this.httpServer.createContext("/api/bans", (exchange -> {
            try {
                String method = exchange.getRequestMethod();

                if ("POST".equals(method)) {
                    handleBan(exchange);
                } else if ("DELETE".equals(method)) {
                    handleUnban(exchange);
                } else {
                    exchange.sendResponseHeaders(405, -1);
                }
            } catch (Exception e) {
                log.error("Ошибка обработки запроса: {}", e.getMessage());
                sendResponse(exchange, 500, "Ошибка обработки запроса: " + e.getMessage());
            } finally {
                exchange.close();
            }
        }));
    }

    /**
     * Добавление запрета
     */
    private static void handleBan(HttpExchange exchange) throws IOException {
        if (!"/api/bans".equals(exchange.getRequestURI().getPath())) {
            sendResponse(exchange, 404, "Не найдено");
            return;
        }

        try (InputStream is = exchange.getRequestBody()) {
            BannedProduct bannedProduct = JsonDeserializer.fromBytesToObject(is.readAllBytes(), BannedProduct.class);

            if (bannedProduct == null || bannedProduct.getProductId() == null || bannedProduct.getProductId().isBlank()) {
                sendResponse(exchange, 400, "Поле product_id обязательно");
                return;
            }

            bannedProduct.setProductId(bannedProduct.getProductId().trim());

            if (bannedProduct.getBannedAt() == null || bannedProduct.getBannedAt().isBlank()) {
                bannedProduct.setBannedAt(Instant.now().toString());
            }

            KafkaMessageProducer.getInstance().sendBan(bannedProduct);
            log.info("Добавлен запрет: {}", bannedProduct.getProductId());

            sendResponse(exchange, 200, "Запрет добавлен: " + bannedProduct.getProductId());
        } catch (Exception e) {
            log.error("Ошибка добавления запрета: {}", e.getMessage());
            sendResponse(exchange, 500, "Ошибка добавления запрета: " + e.getMessage());
        }
    }

    /**
     * Снятие запрета
     */
    private static void handleUnban(HttpExchange exchange) throws IOException {
        String productId = null;
        String path = exchange.getRequestURI().getPath();

        if (path.startsWith("/api/bans/")) {
            productId = path.substring("/api/bans/".length());
        }

        if (productId == null || productId.isBlank()) {
            sendResponse(exchange, 400, "Укажите product_id в пути: DELETE /api/bans/{product_id}");
            return;
        }

        productId = URLDecoder.decode(productId, StandardCharsets.UTF_8).trim();

        if (productId.isEmpty()) {
            sendResponse(exchange, 400, "Укажите product_id в пути: DELETE /api/bans/{product_id}");
            return;
        }

        try {
            KafkaMessageProducer.getInstance().sendUnban(productId);
            log.info("Снят запрет: {}", productId);

            sendResponse(exchange, 200, "Запрет снят: " + productId);
        } catch (Exception e) {
            log.error("Ошибка снятия запрета: {}", e.getMessage());
            sendResponse(exchange, 500, "Ошибка снятия запрета: " + e.getMessage());
        }
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
        if (banHttpServer != null) {
            banHttpServer.httpServer.stop(5);
            log.info("Сервер остановлен");
        }
    }
}
