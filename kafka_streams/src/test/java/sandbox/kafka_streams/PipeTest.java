package sandbox.kafka_streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class PipeTest {

    @Test
    public void shouldPipe() {
        Topology topology = Pipe.createTopology();
        Properties props = Pipe.createProps();
        props.put(StreamsConfig.STATE_DIR_CONFIG, org.apache.kafka.test.TestUtils.tempDirectory().getAbsolutePath());
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);

        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(
                Pipe.INPUT_TOPIC,
                new StringSerializer(),
                new StringSerializer()
        );
        List<KeyValue<String, String>> expectedKeyValues = Arrays.asList(
                new KeyValue<>(null, "Hello Kafka Streams"),
                new KeyValue<>(null, "All streams lead to Kafka"),
                new KeyValue<>(null, "Join Kafka Summit")
        );
        testDriver.pipeInput(factory.create(expectedKeyValues));

        List<KeyValue<String, String>> actualKeyValues = new ArrayList<>();
        while (true) {
            ProducerRecord<String, String> outputRecord = testDriver.readOutput(
                    Pipe.OUTPUT_TOPIC,
                    new StringDeserializer(),
                    new StringDeserializer()
            );
            if (outputRecord == null) {
                break;
            }
            actualKeyValues.add(new KeyValue<>(outputRecord.key(), outputRecord.value()));
        }
        Assert.assertEquals(expectedKeyValues, actualKeyValues);
    }
}
