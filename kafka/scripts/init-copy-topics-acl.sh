#!/bin/bash
BS=copy-kafka-1:29092
CFG=/etc/kafka/jaas/admin.properties

# Топик client api
kafka-topics --bootstrap-server $BS --command-config $CFG --create --if-not-exists \
    --topic $CLIENT_TOPIC_NAME \
    --partitions 3 \
    --replication-factor 3 \
    --config cleanup.policy=delete \
    --config retention.ms=604800000 \
    --config segment.bytes=1073741824 \
    --config min.insync.replicas=2

# Топик connect result
kafka-topics --bootstrap-server $BS --command-config $CFG --create --if-not-exists \
    --topic $FILTERED_TOPIC_NAME \
    --partitions 3 \
    --replication-factor 3 \
    --config cleanup.policy=delete \
    --config retention.ms=604800000 \
    --config segment.bytes=1073741824 \
    --config min.insync.replicas=2

# Топик analyst
kafka-topics --bootstrap-server $BS --command-config $CFG --create --if-not-exists \
    --topic $ANALYST_TOPIC_NAME \
    --partitions 3 \
    --replication-factor 3 \
    --config "cleanup.policy=compact,delete" \
    --config retention.ms=604800000 \
    --config segment.bytes=1073741824 \
    --config min.insync.replicas=2

# Для уже существующего топика: оставляем последнюю рекомендацию на user_id
kafka-topics --bootstrap-server $BS --command-config $CFG --alter \
    --topic $ANALYST_TOPIC_NAME \
    --config "cleanup.policy=compact,delete"

# Топик Schema Registry
kafka-topics --bootstrap-server $BS --command-config $CFG --create --if-not-exists \
    --topic schemas \
    --partitions 1 \
    --replication-factor 3 \
    --config cleanup.policy=compact \
    --config min.insync.replicas=2

# Права mirror maker
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$MIRROR_MAKER_USERNAME --resource-pattern-type prefixed --topic mirror-maker- \
     --group '*' --operation All
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$MIRROR_MAKER_USERNAME --topic $CLIENT_TOPIC_NAME --topic $FILTERED_TOPIC_NAME \
     --operation describe --operation write --operation alterConfigs
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$MIRROR_MAKER_USERNAME --resource-pattern-type prefixed --topic mirror-maker- \
     --group mirror-maker-group --operation All

# Права analyst
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$ANALYST_CONSUMER_USERNAME --topic $CLIENT_TOPIC_NAME --topic $FILTERED_TOPIC_NAME \
    --group '*' --operation describe --operation read
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$ANALYST_CONSUMER_USERNAME --topic $ANALYST_TOPIC_NAME --operation describe --operation write

# Права client api
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$CUSTOMER_PRODUCER_USERNAME --topic $ANALYST_TOPIC_NAME \
    --group '*' --operation describe --operation read

# Права schema-registry
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$SCHEMA_REGISTRY_USERNAME --operation All --topic schemas
kafka-acls --bootstrap-server $BS --command-config $CFG --add \
    --allow-principal User:$SCHEMA_REGISTRY_USERNAME --operation All --group '*'

kafka-topics --bootstrap-server $BS --command-config $CFG --describe --topic $ANALYST_TOPIC_NAME
