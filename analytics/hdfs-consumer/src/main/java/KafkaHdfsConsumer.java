
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import config.HdfsConsumerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.WakeupException;
import java.io.IOException;


import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaHdfsConsumer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter FILE_TS =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    public static void main(String[] args) {
        HdfsConsumerConfig config = HdfsConsumerConfig.getInstance();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config.toConsumerProperties());

        consumer.subscribe(List.of(config.getFilteredProductsTopicName(), config.getClientQueryTopicName()));

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            try {
                consumer.wakeup();
            } catch (Exception ignored) {
            }
        }, "hdfs-consumer-shutdown"));

        Configuration hdfsConf = new Configuration();
        hdfsConf.set("fs.defaultFS", config.getHdfsUri());
        hdfsConf.set("dfs.replication", "3");
        hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hdfsConf.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");

        try (FileSystem hdfs = FileSystem.get(URI.create(config.getHdfsUri()), hdfsConf, config.getHdfsUser())) {
            Path rawRoot = new Path(config.getHdfsRawPath());
            hdfs.mkdirs(new Path(rawRoot, "products"));
            hdfs.mkdirs(new Path(rawRoot, "queries"));

            while (running.get()) {
                ConsumerRecords<String, String> records;

                try {
                    records = consumer.poll(Duration.ofMillis(100));
                } catch (WakeupException e) {
                    if (!running.get()) break;
                    throw e;
                }

                if (records.isEmpty()) continue;

                writeBatch(hdfs, rawRoot, config, records);

                consumer.commitSync();
                log.info("Записано и закоммичено {} сообщений", records.count());
            }
        } catch (WakeupException e) {
            if (running.get()) {
                log.error("Неожиданный wakeup", e);
                throw e;
            }
        } catch (Exception e) {
            log.error("Ошибка consumer в HDFS", e);
            throw new RuntimeException(e);
        } finally {
            try {
                consumer.close();
            } catch (Exception e) {
                log.warn("Ошибка закрытия consumer: {}", e.getMessage());
            }
        }
    }

    private static void writeBatch(FileSystem hdfs, Path rawRoot,
                                   HdfsConsumerConfig config,
                                   ConsumerRecords<String, String> records) throws IOException {
        Map<String, List<ConsumerRecord<String, String>>> byTopic = new LinkedHashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            byTopic.computeIfAbsent(record.topic(), k -> new ArrayList<>()).add(record);
        }

        for (Map.Entry<String, List<ConsumerRecord<String, String>>> entry : byTopic.entrySet()) {
            String subdir = subdirForTopic(config, entry.getKey());
            if (subdir == null) {
                log.warn("Неизвестный топик: {}", entry.getKey());
                continue;
            }
            writeTopicFile(hdfs, new Path(rawRoot, subdir), subdir, entry.getValue());
        }
    }

    private static String subdirForTopic(HdfsConsumerConfig config, String topic) {
        if (topic.equals(config.getFilteredProductsTopicName())) return "products";
        if (topic.equals(config.getClientQueryTopicName())) return "queries";
        return null;
    }

    private static void writeTopicFile(FileSystem hdfs, Path dir, String prefix,
                                       List<ConsumerRecord<String, String>> records) throws IOException {
        hdfs.mkdirs(dir);

        String fileName = prefix + "-" + FILE_TS.format(Instant.now())
                + "-" + UUID.randomUUID() + ".jsonl";
        Path tmp = new Path(dir, "." + fileName + ".tmp");
        Path dst = new Path(dir, fileName);

        try (FSDataOutputStream out = hdfs.create(tmp, false)) {
            for (ConsumerRecord<String, String> record : records) {
                String line = toEnvelope(record);
                if (line == null) continue;
                out.write(line.getBytes(StandardCharsets.UTF_8));
                out.write('\n');
            }
        } catch (IOException e) {
            deleteQuietly(hdfs, tmp);
            throw e;
        } catch (RuntimeException e) {
            deleteQuietly(hdfs, tmp);
            throw e;
        }

        if (!hdfs.rename(tmp, dst)) {
            deleteQuietly(hdfs, tmp);
            throw new IOException("Не удалось переименовать " + tmp + " в " + dst);
        }
        log.info("Записан {} ({} сообщений)", dst, records.size());
    }

    private static void deleteQuietly(FileSystem hdfs, Path path) {
        try {
            hdfs.delete(path, false);
        } catch (IOException e) {
            log.warn("Не удалось удалить {}: {}", path, e.getMessage());
        }
    }

    private static String toEnvelope(ConsumerRecord<String, String> record) {
        if (record.value() == null) return null;

        try {
            JsonNode value = MAPPER.readTree(record.value());
            if (value == null || !value.isObject()) {
                log.error("Ожидался JSON-объект: {} partition={} offset={}",
                        record.topic(), record.partition(), record.offset());
                return null;
            }

            ObjectNode envelope = MAPPER.createObjectNode();
            envelope.put("topic", record.topic());
            envelope.put("partition", record.partition());
            envelope.put("offset", record.offset());
            if (record.key() == null) {
                envelope.putNull("key");
            } else {
                envelope.put("key", record.key());
            }
            envelope.put("timestamp", record.timestamp());
            envelope.set("value", value);

            return MAPPER.writeValueAsString(envelope);
        } catch (Exception e) {
            log.error("Некорректный JSON: {} partition={} offset={}: {}",
                    record.topic(), record.partition(), record.offset(), e.getMessage());
            return null;
        }
    }
}