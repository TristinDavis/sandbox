package sandbox.kafka_streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class LineSplitTest {

    @Test
    public void shouldSplitLines() {
        Topology topology = LineSplit.buildStream(new StreamsBuilder()).build();
        Properties config = LineSplit.createProps();
        config.put(StreamsConfig.STATE_DIR_CONFIG, org.apache.kafka.test.TestUtils.tempDirectory().getAbsolutePath());
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, config);

        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(
                LineSplit.INPUT_TOPIC,
                new StringSerializer(),
                new StringSerializer()
        );
        testDriver.pipeInput(factory.create(Arrays.asList(
                new KeyValue<>(null, "Hello Kafka Streams"),
                new KeyValue<>(null, "All streams lead to Kafka"),
                new KeyValue<>(null, "Join Kafka Summit")
        )));

        List<String> expectedValues = new ArrayList<>();
        expectedValues.add("Hello");
        expectedValues.add("Kafka");
        expectedValues.add("Streams");
        expectedValues.add("All");
        expectedValues.add("streams");
        expectedValues.add("lead");
        expectedValues.add("to");
        expectedValues.add("Kafka");
        expectedValues.add("Join");
        expectedValues.add("Kafka");
        expectedValues.add("Summit");

        List<String> actualValues= new ArrayList<>();
        while (true) {
            ProducerRecord<String, String> outputRecord = testDriver.readOutput(
                    LineSplit.OUTPUT_TOPIC,
                    new StringDeserializer(),
                    new StringDeserializer()
            );
            if (outputRecord == null) {
                break;
            }
            actualValues.add(outputRecord.value());
        }
        Assert.assertEquals(expectedValues, actualValues);
    }
}
