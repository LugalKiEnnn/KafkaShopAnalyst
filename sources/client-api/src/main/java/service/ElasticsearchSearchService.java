package service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import config.ClientApiConfig;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/**
 * Поиск товаров в Elasticsearch
 */
public class ElasticsearchSearchService {
    private static volatile ElasticsearchSearchService instance;

    private final String baseUrl;
    private final String index;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public static ElasticsearchSearchService getInstance() {
        if (instance == null) {
            synchronized (ElasticsearchSearchService.class) {
                if (instance == null) {
                    instance = new ElasticsearchSearchService();
                }
            }
        }
        return instance;
    }

    private ElasticsearchSearchService() {
        ClientApiConfig config = ClientApiConfig.getInstance();

        String url = config.getElasticsearchUrl();
        String indexName = config.getElasticsearchIndex();

        if (url == null || url.isBlank()) {
            throw new IllegalStateException("Не задан elasticsearch.url (ELASTICSEARCH_URL)");
        }

        this.baseUrl = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
        this.index = (indexName == null || indexName.isBlank()) ? "products" : indexName;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
    }

    /**
     * Поиск товаров по имени/описанию/бренду/категории/тегам
     * @param query строка поиска
     * @return JSON с полями query, total и items
     */
    public String search(String query) throws IOException, InterruptedException {
        ObjectNode multiMatch = objectMapper.createObjectNode();
        multiMatch.put("query", query);
        multiMatch.set("fields", objectMapper.valueToTree(
                List.of("name^3", "description", "brand", "category", "tags")));

        ObjectNode queryNode = objectMapper.createObjectNode();
        queryNode.set("multi_match", multiMatch);

        ObjectNode requestBody = objectMapper.createObjectNode();
        requestBody.put("size", 20);
        requestBody.set("query", queryNode);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/" + index + "/_search"))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        objectMapper.writeValueAsString(requestBody), StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> response = httpClient.send(
                request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Elasticsearch вернул HTTP " + response.statusCode()
                    + ": " + response.body());
        }

        JsonNode root = objectMapper.readTree(response.body());
        JsonNode hits = root.path("hits");
        JsonNode totalNode = hits.path("total");
        long total = totalNode.isObject() ? totalNode.path("value").asLong() : totalNode.asLong();

        ArrayNode items = objectMapper.createArrayNode();
        for (JsonNode hit : hits.path("hits")) {
            items.add(hit.path("_source"));
        }

        ObjectNode result = objectMapper.createObjectNode();
        result.put("query", query);
        result.put("total", total);
        result.set("items", items);

        return objectMapper.writeValueAsString(result);
    }
}
