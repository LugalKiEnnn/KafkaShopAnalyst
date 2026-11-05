#!/bin/bash
BS=kafka-1:29092
CFG=/etc/kafka/jaas/admin.properties

# Топик shop api
kafka-topics --bootstrap-server $BS --command-config $CFG --create --if-not-exists \
    --topic $SHOP_TOPIC_NAME \
    --partitions 3 \
    --replication-factor 3 \
    --config cleanup.policy=delete \
    --config retention.ms=604800000 \
    --config segment.bytes=1073741824 \
    --config min.insync.replicas=2

# Топик client api
kafka-topics --bootstrap-server $BS --command-config $CFG --create --if-not-exists \
    --topic $CLIENT_TOPIC_NAME \
    --partitions 3 \
    --replication-factor 3 \
    --config cleanup.policy=delete \
    --config retention.ms=604800000 \
    --config segment.bytes=1073741824 \
    --config min.insync.replicas=2

# Топик ban api
kafka-topics --bootstrap-server $BS --command-config $CFG --create --if-not-exists \
    --topic $BANNED_TOPIC_NAME \
    --partitions 3 \
    --replication-factor 3 \
    --config cleanup.policy=compact \
    --config min.insync.replicas=2

# Топик streams result
kafka-topics --bootstrap-server $BS --command-config $CFG --create --if-not-exists \
    --topic $FILTERED_TOPIC_NAME \
    --partitions 3 \
    --replication-factor 3 \
    --config cleanup.policy=delete \
    --config retention.ms=604800000 \
    --config segment.bytes=1073741824 \
    --config min.insync.replicas=2

# Топик Mirror Maker
kafka-topics --bootstrap-server $BS --command-config $CFG --create --if-not-exists \
    --topic mm2-offset-syncs.copy.internal \
    --partitions 1 \
    --replication-factor 3 \
    --config cleanup.policy=compact \
    --config min.insync.replicas=2

# Топик Schema Registry
kafka-topics --bootstrap-server $BS --command-config $CFG --create --if-not-exists \
    --topic schemas \
    --partitions 1 \
    --replication-factor 3 \
    --config cleanup.policy=compact \
    --config min.insync.replicas=2

# Права shop-producer
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$SHOP_PRODUCER_USERNAME --topic $SHOP_TOPIC_NAME --operation describe --operation write

# Права customer-producer
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$CUSTOMER_PRODUCER_USERNAME --topic $CLIENT_TOPIC_NAME --operation describe --operation write

# Права ban-producer
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$BAN_PRODUCER_USERNAME --topic $BANNED_TOPIC_NAME --operation describe --operation write

# Права streams
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$STREAMS_USERNAME --topic $SHOP_TOPIC_NAME  --topic $BANNED_TOPIC_NAME \
    --group '*' --operation describe --operation read
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$STREAMS_USERNAME --topic $FILTERED_TOPIC_NAME --operation describe --operation write
# Внутренние топики Kafka Streams: changelog/repartition (processor-...)
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$STREAMS_USERNAME \
    --resource-pattern-type prefixed --topic processor- --operation All

# Права connect
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$CONNECT_USERNAME --topic $FILTERED_TOPIC_NAME \
    --group '*' --operation describe --operation read
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$CONNECT_USERNAME \
    --resource-pattern-type prefixed --topic connect- --operation All

# Права mirror maker
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$MIRROR_MAKER_USERNAME --topic $FILTERED_TOPIC_NAME --topic $CLIENT_TOPIC_NAME \
    --operation describe --operation read --operation describeConfigs
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$MIRROR_MAKER_USERNAME --topic $FILTERED_TOPIC_NAME --topic $CLIENT_TOPIC_NAME \
    --group '*' --operation describe --operation read
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$MIRROR_MAKER_USERNAME --topic mm2-offset-syncs.copy.internal --operation describe --operation write

# Права schema-registry
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$SCHEMA_REGISTRY_USERNAME --operation All --topic schemas
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$SCHEMA_REGISTRY_USERNAME --operation All --group '*'

kafka-topics --bootstrap-server $BS --command-config $CFG --describe --topic $SHOP_TOPIC_NAME
kafka-topics --bootstrap-server $BS --command-config $CFG --describe --topic $CLIENT_TOPIC_NAME
kafka-topics --bootstrap-server $BS --command-config $CFG --describe --topic $BANNED_TOPIC_NAME
kafka-topics --bootstrap-server $BS --command-config $CFG --describe --topic $FILTERED_TOPIC_NAME
