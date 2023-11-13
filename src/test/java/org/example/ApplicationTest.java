package org.example;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.avro.ElectronicOrder;
import org.junit.Test;

import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static java.time.temporal.ChronoUnit.HOURS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ApplicationTest {

    private static long ts(final String timeString) throws ParseException {
        return Instant.parse(timeString).toEpochMilli();
    }

    @Test
    public void shouldReorderTheInput() throws ParseException {

        // Input records, not ordered by time
        final List<ElectronicOrder> inputValues = new ArrayList<>();
        inputValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("1").setUserId("vandeley").setTime(ts("2021-11-03T23:00:00Z")).setPrice(5.0).build());
        inputValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("2").setUserId("penny-packer").setTime(ts("2021-11-04T01:05:00Z")).setPrice(15.0).build());
        inputValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("3").setUserId("romanov").setTime(ts("2021-11-04T01:10:00Z")).setPrice(25.0).build());
        inputValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("4").setUserId("david").setTime(ts("2021-11-04T01:40:00Z")).setPrice(35.0).build());
        inputValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("5").setUserId("jerry").setTime(ts("2021-11-04T02:25:00Z")).setPrice(45.0).build());
        inputValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("6").setUserId("natalie").setTime(ts("2021-11-04T01:20:00Z")).setPrice(55.0).build());
        inputValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("7").setUserId("lisa").setTime(ts("2021-11-04T02:45:00Z")).setPrice(65.0).build());
        inputValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("8").setUserId("gavin").setTime(ts("2021-11-04T02:00:00Z")).setPrice(75.0).build());
        inputValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("9").setUserId("paul").setTime(ts("2021-11-04T03:00:00Z")).setPrice(85.0).build());
        inputValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("10").setUserId("martin").setTime(ts("2021-11-04T02:40:00Z")).setPrice(95.0).build());
        inputValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("11").setUserId("rebecca").setTime(ts("2021-11-04T02:20:00Z")).setPrice(105.0).build());
        inputValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("12").setUserId("jessica").setTime(ts("2021-11-05T00:00:00Z")).setPrice(115.0).build());

        //  Expected, ordered by time
        final List<ElectronicOrder> expectedValues = new ArrayList<>();
        expectedValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("1").setUserId("vandeley").setTime(ts("2021-11-03T23:00:00Z")).setPrice(5.0).build());
        expectedValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("2").setUserId("penny-packer").setTime(ts("2021-11-04T01:05:00Z")).setPrice(15.0).build());
        expectedValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("3").setUserId("romanov").setTime(ts("2021-11-04T01:10:00Z")).setPrice(25.0).build());
        expectedValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("6").setUserId("natalie").setTime(ts("2021-11-04T01:20:00Z")).setPrice(55.0).build());
        expectedValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("4").setUserId("david").setTime(ts("2021-11-04T01:40:00Z")).setPrice(35.0).build());
        expectedValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("8").setUserId("gavin").setTime(ts("2021-11-04T02:00:00Z")).setPrice(75.0).build());
        expectedValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("11").setUserId("rebecca").setTime(ts("2021-11-04T02:20:00Z")).setPrice(105.0).build());
        expectedValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("5").setUserId("jerry").setTime(ts("2021-11-04T02:25:00Z")).setPrice(45.0).build());
        expectedValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("10").setUserId("martin").setTime(ts("2021-11-04T02:40:00Z")).setPrice(95.0).build());
        expectedValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("7").setUserId("lisa").setTime(ts("2021-11-04T02:45:00Z")).setPrice(65.0).build());
        expectedValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("9").setUserId("paul").setTime(ts("2021-11-04T03:00:00Z")).setPrice(85.0).build());
        expectedValues.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("12").setUserId("jessica").setTime(ts("2021-11-05T00:00:00Z")).setPrice(115.0).build());

        //
        // Step 1: Configure and start the processor topology.
        //
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put("schema.registry.url", "mock://reorder-integration-test");
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "reorder-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");

        final Map<String, Object> configMap =
                StreamsUtils.propertiesToMap(streamsConfiguration);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde =
                StreamsUtils.getSpecificAvroSerde(configMap);

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, electronicSerde.getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, Application.OrderTimestampExtractor.class.getName());

        final String inputTopic = "inputTopic";
        final String outputTopic = "outputTopic";
        final String persistentStore = "reorderStore";

        final StoreBuilder<KeyValueStore<String, ElectronicOrder>> orderStoreSupplier =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(persistentStore),
                        Serdes.String(),
                        electronicSerde);

        builder.addStateStore(orderStoreSupplier);

        final KStream<String, ElectronicOrder> stream = builder.stream(inputTopic);
        final KStream<String, ElectronicOrder> reordered = stream
                .process(new Application.ReorderProcessorSupplier<>(
                        persistentStore,
                        Duration.of(10, HOURS),
                        (k,v) -> String.format("key-%d", v.getTime())
                ), persistentStore);
        reordered.to(outputTopic);

        final Topology topology = builder.build();

        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)) {
            //
            // Step 2: Setup input and output topics.
            //
            final TestInputTopic<String, ElectronicOrder> input = topologyTestDriver
                    .createInputTopic(inputTopic,
                            new Serdes.StringSerde().serializer(),
                            electronicSerde.serializer());

            final TestOutputTopic<String, ElectronicOrder> output = topologyTestDriver
                    .createOutputTopic(outputTopic,
                            new Serdes.StringSerde().deserializer(),
                            electronicSerde.deserializer());

            //
            // Step 3: Produce some input data to the input topic.
            //
            inputValues.forEach(order -> input.pipeInput(order.getElectronicId(), order));

            //
            // Step 4: Verify the application's output data.
            //
            assertThat(output.readValuesToList(), equalTo(expectedValues));
        }
    }

}
