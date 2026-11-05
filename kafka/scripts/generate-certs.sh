#!/bin/bash
source .env

rm -rf secrets
mkdir -p secrets/acl_config
cd secrets

# Создание корневого ключа и сертификата
openssl req -new -nodes -x509 -days 3650 -newkey rsa:2048 -keyout ca.key -out ca.crt -config ../certs-config/ca.cnf

# Группировка сертификата и ключа
cat ca.crt ca.key > ca.pem

for NAME in kafka-1 kafka-2 kafka-3 copy-kafka-1 copy-kafka-2 copy-kafka-3 client mirror-maker schema-registry kafka-ui; do
    mkdir -p $NAME

    # Создание CSR
    openssl req -new \
        -newkey rsa:2048 \
        -keyout $NAME/$NAME.key \
        -out $NAME/$NAME.csr \
        -config ../certs-config/$NAME.cnf \
        -nodes

    # Создание сертификата
    openssl x509 -req \
        -days 3650 \
        -in $NAME/$NAME.csr \
        -CA ca.crt \
        -CAkey ca.key \
        -CAcreateserial \
        -out $NAME/$NAME.crt \
        -extfile ../certs-config/$NAME.cnf \
        -extensions v3_req

    # Создание хранилища сертификатов
    openssl pkcs12 -export \
        -in $NAME/$NAME.crt \
        -inkey $NAME/$NAME.key \
        -chain \
        -CAfile ca.pem \
        -name $NAME \
        -out $NAME/$NAME.p12 \
        -password pass:$CERT_PASSWORD

    # Создание keystore
    keytool -importkeystore \
        -srckeystore $NAME/$NAME.p12 \
        -srcstoretype PKCS12 \
        -destkeystore $NAME/$NAME.keystore.jks \
        -deststoretype JKS \
        -deststorepass $CERT_PASSWORD \
        -noprompt \
        -srcstorepass $CERT_PASSWORD

    # Создание truststore
    keytool -import \
        -trustcacerts \
        -storetype JKS \
        -alias ca \
        -file ca.crt \
        -keystore $NAME/$NAME.truststore.jks \
        -storepass $CERT_PASSWORD \
        -noprompt

    echo $CERT_PASSWORD > $NAME/${NAME}_sslkey_creds
done

# JAAS ZooKeeper
cat > acl_config/zookeeper.sasl.jaas.conf <<JAAS
Server {
    org.apache.zookeeper.server.auth.DigestLoginModule required
    user_$ZOOKEEPER_USERNAME="$ZOOKEEPER_PASSWORD";
};
JAAS

# JAAS брокеров
cat > acl_config/kafka_server_jaas.conf <<JAAS
KafkaServer {
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="$ADMIN_USERNAME"
    password="$ADMIN_PASSWORD";
};

Client {
    org.apache.zookeeper.server.auth.DigestLoginModule required
    username="$ZOOKEEPER_USERNAME"
    password="$ZOOKEEPER_PASSWORD";
};
JAAS

# Настройки администратора
cat > acl_config/admin.properties <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$ADMIN_USERNAME" password="$ADMIN_PASSWORD";
ssl.keystore.location=/etc/kafka/client/client.keystore.jks
ssl.keystore.password=$CERT_PASSWORD
ssl.key.password=$CERT_PASSWORD
ssl.truststore.location=/etc/kafka/client/client.truststore.jks
ssl.truststore.password=$CERT_PASSWORD
ssl.endpoint.identification.algorithm=https
PROPS


cd ..