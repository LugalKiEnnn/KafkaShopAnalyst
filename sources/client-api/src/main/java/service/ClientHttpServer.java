package service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import config.ClientApiConfig;
import kafka.KafkaAnalystConsumer;
import kafka.KafkaMessageProducer;
import lombok.extern.slf4j.Slf4j;
import model.ClientRequest;
import model.RequestType;
import util.JsonDeserializer;
import util.JsonSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

/**
 * HTTP сервер CLIENT API
 */
@Slf4j
public class ClientHttpServer {
    private static volatile ClientHttpServer clientHttpServer;
    private final HttpServer httpServer;

    /**
     * Получить экземпляр http сервера
     */
    public static ClientHttpServer getInstance() throws IOException {
        if (clientHttpServer == null) {
            synchronized (ClientHttpServer.class) {
                clientHttpServer = new ClientHttpServer();
            }
        }
        return clientHttpServer;
    }

    private ClientHttpServer() throws IOException {
        this.httpServer = HttpServer.create(new InetSocketAddress(ClientApiConfig.getInstance().getHttpPort()), 0);

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

        // Поиск товара по имени
        this.httpServer.createContext("/api/search", (exchange -> {
            if ("POST".equals(exchange.getRequestMethod())) {
                try (InputStream is = exchange.getRequestBody()) {
                    ClientRequest request = readRequest(is, RequestType.SEARCH);

                    log.info("Запрос клиента: {}", JsonSerializer.toJsonString(request));

                    if (isBlank(request.getUserId())) {
                        sendJsonResponse(exchange, 400, errorJson("Поле user_id обязательно"));
                    } else {
                        String query = request.getQuery();
                        if (query == null || query.isBlank()) {
                            sendJsonResponse(exchange, 400, errorJson("Поле query обязательно"));
                        } else {
                            KafkaMessageProducer.getInstance().sendRequest(request);

                            String result = ElasticsearchSearchService.getInstance().search(query);

                            sendJsonResponse(exchange, 200, result);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    sendJsonResponse(exchange, 500, errorJson("Поиск прерван: " + e.getMessage()));
                } catch (IOException e) {
                    log.error("Ошибка Elasticsearch: {}", e.getMessage());
                    sendJsonResponse(exchange, 502, errorJson("Elasticsearch недоступен: " + e.getMessage()));
                } catch (Exception e) {
                    log.error("Ошибка при поиске: {}", e.getMessage());
                    sendJsonResponse(exchange, 500, errorJson("Ошибка при поиске: " + e.getMessage()));
                }
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
            exchange.close();
        }));

        // Получение рекомендаций
        this.httpServer.createContext("/api/recommendations", (exchange -> {
            if ("POST".equals(exchange.getRequestMethod())) {
                try (InputStream is = exchange.getRequestBody()) {
                    ClientRequest request = readRequest(is, RequestType.RECOMMENDATIONS);

                    log.info("Запрос клиента: {}", JsonSerializer.toJsonString(request));

                    if (isBlank(request.getUserId())) {
                        sendJsonResponse(exchange, 400, errorJson("Поле user_id обязательно"));
                    } else {
                        KafkaMessageProducer.getInstance().sendRequest(request);

                        String recommendation = KafkaAnalystConsumer.getInstance().getRecommendationByRequest(request);

                        if (recommendation == null || recommendation.isBlank()) {
                            sendJsonResponse(exchange, 404, errorJson("Рекомендации для пользователя не найдены"));
                        } else {
                            sendJsonResponse(exchange, 200, recommendation);
                        }
                    }
                } catch (Exception e) {
                    log.error("Ошибка при получении рекомендаций: {}", e.getMessage());
                    sendJsonResponse(exchange, 500, errorJson("Ошибка при получении рекомендаций: " + e.getMessage()));
                }
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
            exchange.close();
        }));
    }

    private static ClientRequest readRequest(InputStream is, RequestType type) throws IOException {
        ClientRequest request = JsonDeserializer.fromBytesToObject(is.readAllBytes(), ClientRequest.class);
        request.setType(type);
        request.setTimestamp(Instant.now().toString());
        return request;
    }

    private static void sendResponse(HttpExchange exchange, int code, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
        exchange.sendResponseHeaders(code, bytes.length);
        OutputStream output = exchange.getResponseBody();
        output.write(bytes);
        output.flush();
    }

    private static void sendJsonResponse(HttpExchange exchange, int code, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(code, bytes.length);
        OutputStream output = exchange.getResponseBody();
        output.write(bytes);
        output.flush();
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static String errorJson(String message) {
        return JsonSerializer.toJsonString(Map.of("error", message == null ? "unknown error" : message));
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
        if (clientHttpServer != null) {
            clientHttpServer.httpServer.stop(5);
            log.info("Сервер остановлен");
        }
    }
}
