package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.example.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import static org.example.StreamsUtils.*;

import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.time.temporal.ChronoUnit.HOURS;

public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static class ReorderProcessorSupplier<K, V> implements ProcessorSupplier<K, V, K, V> {

        public interface StoreKeyGenerator<K, V> {
            K getStoreKey(K key, V val);
        }

        private final String storeName;
        private final Duration grace;

        private final StoreKeyGenerator<K,V> storeKeyGenerator;

        public ReorderProcessorSupplier(String storeName, Duration grace, StoreKeyGenerator<K, V> storeKeyGenerator) {
            this.storeName = storeName;
            this.grace = grace;
            this.storeKeyGenerator = storeKeyGenerator;
        }

        @Override
        public Processor<K, V, K, V> get() {
            return new Processor<K, V, K, V>() {

                private KeyValueStore<K, V> reorderStore;
                private ProcessorContext<K, V> context;

                @Override
                public void init(ProcessorContext<K, V> context) {
                    reorderStore = context.getStateStore(storeName);
                    this.context = context;
                    context.schedule(
                            grace,
                            PunctuationType.STREAM_TIME,
                            this::punctuate
                    );
                }

                void punctuate(final long timestamp) {
                    try(KeyValueIterator<K, V> it = reorderStore.all()) {
                        // Iterate over the records and create a Record instance and forward downstream
                        while(it.hasNext()) {
                            KeyValue<K, V> kv = it.next();
                            Record<K, V> rec = new Record<>(kv.key, kv.value, timestamp);
                            context.forward(rec);
                            reorderStore.delete(kv.key);
                            logger.info("Punctuation forwarded record - key " + rec.key() + " value " + rec.value());
                        }
                    }
                }

                @Override
                public void process(Record<K, V> record) {
                    // Keys need to contain and be sortable by time
                    final K storeKey = storeKeyGenerator.getStoreKey(record.key(), record.value());
                    final V storeValue = reorderStore.get(storeKey);
                    if(storeValue == null) {
                        logger.info("Processed record - key " + record.key() + " value " + record.value());
                        reorderStore.put(storeKey, record.value());
                    }
                }
            };
        }
    }

    public static class OrderTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            // Extract the timestamp from the value in the record
            // and return that instead
            ElectronicOrder order = (ElectronicOrder) record.value();
            logger.info("Extracting time of " + order.getTime() + " from " + order);
            return order.getTime();
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to a configuration file.");
        }

        final StreamsBuilder builder = new StreamsBuilder();

        final Properties streamsProps = loadProperties(args[0]);
        final Map<String, Object> configMap = propertiesToMap(streamsProps);

        final Serde<String> stringSerde = Serdes.String();
        final SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);

        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "reorder-api-application");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, electronicSerde.getClass().getName());
        streamsProps.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimestampExtractor.class.getName());

        final String inputTopic = streamsProps.getProperty("input.topic.name");
        final String outputTopic = streamsProps.getProperty("output.topic.name");
        final String persistentStore = "reorderStore";

        final StoreBuilder<KeyValueStore<String, ElectronicOrder>> orderStoreSupplier =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(persistentStore),
                        stringSerde,
                        electronicSerde);

        builder.addStateStore(orderStoreSupplier);

        final KStream<String, ElectronicOrder> stream = builder.stream(inputTopic);
        final KStream<String, ElectronicOrder> reordered = stream
                .process(new ReorderProcessorSupplier<>(
                        persistentStore,
                        Duration.of(10, HOURS),
                        (k,v) -> String.format("key-%d", v.getTime())
                ), persistentStore);
        reordered.to(outputTopic);

        final Topology topology = builder.build();

        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));

            // Clean local state prior to starting the processing topology.
            kafkaStreams.cleanUp();

            try {
                logger.info("Starting application");
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }

}
