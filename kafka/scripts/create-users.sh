#!/bin/bash
kafka-configs --zookeeper zookeeper:2181 --alter --entity-type users --entity-name $ADMIN_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$ADMIN_PASSWORD]"
kafka-configs --zookeeper zookeeper:2181 --alter --entity-type users --entity-name $SHOP_PRODUCER_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$SHOP_PRODUCER_PASSWORD]"
kafka-configs --zookeeper zookeeper:2181 --alter --entity-type users --entity-name $CUSTOMER_PRODUCER_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$CUSTOMER_PRODUCER_PASSWORD]"
kafka-configs --zookeeper zookeeper:2181 --alter --entity-type users --entity-name $BAN_PRODUCER_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$BAN_PRODUCER_PASSWORD]"
kafka-configs --zookeeper zookeeper:2181 --alter --entity-type users --entity-name $STREAMS_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$STREAMS_PASSWORD]"
kafka-configs --zookeeper zookeeper:2181 --alter --entity-type users --entity-name $CONNECT_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$CONNECT_PASSWORD]"
kafka-configs --zookeeper zookeeper:2181 --alter --entity-type users --entity-name $MIRROR_MAKER_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$MIRROR_MAKER_PASSWORD]"
kafka-configs --zookeeper zookeeper:2181 --alter --entity-type users --entity-name $SCHEMA_REGISTRY_USERNAME \
    --add-config "SCRAM-SHA-512=[password=$SCHEMA_REGISTRY_PASSWORD]"
