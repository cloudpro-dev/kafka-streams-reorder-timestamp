# Reorder by timestamp demo

This demo shows how we can consume from one Kafka topic and based on the message timestamp and re-order the messages into the chronological order on the Kafka output topic.

The `grace` period defines how long the consumer will wait before forwarding records to the output topic.  The period is based upon the record timestamp, not the wall clock.

That means that as soon as all the records within this period are available on the input topic, then all records within the period will be forwarded.

This sample uses the following technologies:
- Kafka - Record storage
- Kafka streams - Record processing
- Avro - Record schema definition

## Running the sample

To run the sample you will require a running Kafka cluster.

To start a working Kafka cluster in Docker for use with this sample, you can use the following command:
```shell
docker compose -f docker.yml up -d
```

First we need to create the input and output topic in Kafka:
```shell
docker run --rm --network kafka-streams-reorder-timestamp_kafka-net -it bitnami/kafka:3.5 bash
$ /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server=kafka-streams-reorder-timestamp-kafka-1:9092 --create --topic reorder-input-topic
$ /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server=kafka-streams-reorder-timestamp-kafka-1:9092 --create --topic reorder-output-topic
exit
```

Next we need to populate a topic with some messages to be consumed by the application.  These messages should be added
with timestamps that are not in chronological order.
```shell
docker run --rm -it --network kafka-streams-reorder-timestamp_kafka-net confluentinc/cp-schema-registry:7.4.0 bash

$ kafka-avro-console-producer \
--broker-list kafka-streams-reorder-timestamp-kafka-1:9092 \
--topic reorder-input-topic \
--property value.schema='{"namespace": "org.example.avro", "type": "record", "name": "ElectronicOrder", "fields": [{"name": "order_id", "type": "string" }, {"name": "electronic_id", "type": "string" }, {"name": "user_id", "type": "string" }, {"name": "price", "type": "double", "default": 0.0 }, {"name": "time", "type": "long" } ] }' \
--property schema.registry.url=http://kafka-streams-reorder-timestamp-schema-registry-1:8081 \
--property "parse.key=true" \
--property "key.separator=:" \
--property "key.serializer=org.apache.kafka.common.serialization.StringSerializer"

HDTV-2333 : {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "10261998", "price": 2000.0, "time": 1635980400000 }
HDTV-2333 : {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1033737373", "price": 1999.23, "time": 1635987900000 }
HDTV-2333 : {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1026333", "price": 4500.0, "time": 1635988200000 }
HDTV-2333 : {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1333.98, "time": 1635990000000 }
HDTV-2333 : {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1345.55, "time": 1635992700000 }
HDTV-2333 : {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 5333.98, "time": 1635988800000 }
HDTV-2333 : {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 4333.98, "time": 1635993900000 }
HDTV-2333 : {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1000.0, "time": 1635991200000 }
HDTV-2333 : {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1254.87, "time": 1635994800000 }
HDTV-2333 : {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 2564.65, "time": 1635993600000 }
HDTV-2333 : {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 658.45, "time": 1635992400000 }
HDTV-2333 : {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 3331.25, "time": 1636070400000 }
```

Once you have some data on the input stream you can start the Kafka stream application to start processing the streaming data.
```shell
mvn clean package
java -cp target/kafka-streams-reorder-timestamp-jar-with-dependencies.jar org.example.Application configuration/dev.properties
```

To view the reordered messages as they are produced, start another instance of Kafka client and run the following command:
```shell
docker run \
  --rm \
  --network kafka-streams-reorder-timestamp_kafka-net \
  confluentinc/cp-schema-registry:7.4.0 \
  kafka-avro-console-consumer \
    --bootstrap-server kafka-streams-reorder-timestamp-kafka-1:9092 \
    --topic reorder-output-topic \
    --from-beginning \
    --property schema.registry.url=http://kafka-streams-reorder-timestamp-schema-registry-1:8081
...
{"order_id":"instore-1","electronic_id":"HDTV-2333","user_id":"10261998","price":2000.0,"time":1635980400000}
{"order_id":"instore-1","electronic_id":"HDTV-2333","user_id":"1033737373","price":1999.23,"time":1635987900000}
{"order_id":"instore-1","electronic_id":"HDTV-2333","user_id":"1026333","price":4500.0,"time":1635988200000}
{"order_id":"instore-1","electronic_id":"HDTV-2333","user_id":"1038884844","price":5333.98,"time":1635988800000}
{"order_id":"instore-1","electronic_id":"HDTV-2333","user_id":"1038884844","price":1333.98,"time":1635990000000}
{"order_id":"instore-1","electronic_id":"HDTV-2333","user_id":"1038884844","price":1000.0,"time":1635991200000}
{"order_id":"instore-1","electronic_id":"HDTV-2333","user_id":"1038884844","price":658.45,"time":1635992400000}
{"order_id":"instore-1","electronic_id":"HDTV-2333","user_id":"1038884844","price":1345.55,"time":1635992700000}
{"order_id":"instore-1","electronic_id":"HDTV-2333","user_id":"1038884844","price":2564.65,"time":1635993600000}
{"order_id":"instore-1","electronic_id":"HDTV-2333","user_id":"1038884844","price":4333.98,"time":1635993900000}
{"order_id":"instore-1","electronic_id":"HDTV-2333","user_id":"1038884844","price":1254.87,"time":1635994800000}
{"order_id":"instore-1","electronic_id":"HDTV-2333","user_id":"1038884844","price":3331.25,"time":1636070400000}
```

To clean up all resources after the tests, run the following command:
```shell
docker compose -f docker.yml down -v
```