#!/bin/bash
kafka-configs --zookeeper copy-zookeeper:2181 --alter --entity-type users --entity-name $ADMIN_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$ADMIN_PASSWORD]"
kafka-configs --zookeeper copy-zookeeper:2181 --alter --entity-type users --entity-name $MIRROR_MAKER_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$MIRROR_MAKER_PASSWORD]"
kafka-configs --zookeeper copy-zookeeper:2181 --alter --entity-type users --entity-name $ANALYST_CONSUMER_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$ANALYST_CONSUMER_PASSWORD]"
kafka-configs --zookeeper copy-zookeeper:2181 --alter --entity-type users --entity-name $SCHEMA_REGISTRY_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$SCHEMA_REGISTRY_PASSWORD]"
kafka-configs --zookeeper copy-zookeeper:2181 --alter --entity-type users --entity-name $CUSTOMER_PRODUCER_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$CUSTOMER_PRODUCER_PASSWORD]"