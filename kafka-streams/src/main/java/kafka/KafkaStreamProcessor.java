package kafka;

import config.KafkaProcessorConfig;
import lombok.extern.slf4j.Slf4j;
import model.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import util.JsonSerializer;
import util.bannedProduct.BannedProductSerdes;
import util.product.ProductSerdes;

/**
 * Класс процессора потоков
 */
@Slf4j
public class KafkaStreamProcessor {
    private static volatile KafkaStreamProcessor kafkaStreamProcessor;

    private final KafkaProcessorConfig config;
    private final KafkaStreams relationStream;

    public static KafkaStreamProcessor getInstance() {
        synchronized (KafkaStreamProcessor.class) {
            if (kafkaStreamProcessor == null) {
                kafkaStreamProcessor = new KafkaStreamProcessor();
            }
        }

        return kafkaStreamProcessor;
    }

    private KafkaStreamProcessor() {
        this.config = KafkaProcessorConfig.getInstance();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Product> productStream = buildProductStream(streamsBuilder);
        KTable<String, BannedProduct> bannedProductsTable = buildBannedProductsTable(streamsBuilder);

        filterProducts(productStream, bannedProductsTable);
        deleteBannedProductsFromSearch(bannedProductsTable);

        Topology topology = streamsBuilder.build();

        log.info("Топология:");
        System.out.println(topology.describe());

        this.relationStream = new KafkaStreams(topology, config.toStreamProperties());
        relationStream.setStateListener((newState, oldState) -> {
            log.info("Kafka Streams: {} -> {}", oldState, newState);
            if (newState == KafkaStreams.State.ERROR) {
                log.error("Kafka Streams перешёл в ERROR");
                System.exit(1);
            }
        });
        this.relationStream.start();
    }

    /**
     * Построение потока продуктов
     */
    private KStream<String, Product> buildProductStream(StreamsBuilder builder) {
        KStream<String, Product> productStream = builder.stream(
                config.getProductsTopicName(),
                Consumed.with(Serdes.String(), new ProductSerdes())
        );

        productStream.foreach((receiverId, product) -> log.info(product.toString()));

        return productStream;
    }

    /**
     * Построение таблицы заблокированных продуктов
     */
    private KTable<String, BannedProduct> buildBannedProductsTable(StreamsBuilder builder) {
        return builder.table(
                config.getBannedProductsTopicName(),
                Consumed.with(Serdes.String(), new BannedProductSerdes()),
                Materialized.<String, BannedProduct, KeyValueStore<Bytes, byte[]>>as(config.getBannedProductsStore())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new BannedProductSerdes())
        );
    }

    /**
     * При бане отправляем tombstone в filtered-product-topic,
     * чтобы Kafka Connect удалил товар из Elasticsearch.
     * Tombstone от снятия бана (banned == null) пропускаем.
     */
    private void deleteBannedProductsFromSearch(KTable<String, BannedProduct> bannedProductsTable) {
        bannedProductsTable.toStream()
                .filter((key, banned) -> banned != null)
                .mapValues((key, banned) -> (String) null)
                .to(config.getFilteredProductsTopicName(), Produced.with(Serdes.String(), Serdes.String()));
    }

    private void filterProducts(KStream<String, Product> productStream, KTable<String, BannedProduct> bannedProductsTable) {
        KStream<String, String> filteredProductStream = productStream
                .leftJoin(bannedProductsTable,
                        (product, bannedProduct) -> {
                            if (bannedProduct != null) {
                                log.info("Продукт {} забанен для использования с {}: {}", product.getName(), bannedProduct.getBannedAt(), bannedProduct.getReason());
                                return null;
                            } else {
                                return product;
                            }
                        }
                )
                .filter((key, product) -> product != null)
                .mapValues((key, product) -> JsonSerializer.toJsonString(product));

        filteredProductStream.to(config.getFilteredProductsTopicName(), Produced.with(Serdes.String(), Serdes.String()));
    }

    public static void closeProcessor() {
        if (kafkaStreamProcessor != null) {
            kafkaStreamProcessor.relationStream.close();
            log.info("Процессор остановлен");
        }
    }
}
