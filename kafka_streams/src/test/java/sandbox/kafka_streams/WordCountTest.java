package sandbox.kafka_streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WordCountTest {

    @Test
    public void shouldCountWords() {
        Topology topology = WordCount.createTopology();
        Properties props = WordCount.createProps();
        props.put(StreamsConfig.STATE_DIR_CONFIG, org.apache.kafka.test.TestUtils.tempDirectory().getAbsolutePath());
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);

        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(
                WordCount.INPUT_TOPIC,
                new StringSerializer(),
                new StringSerializer()
        );
        testDriver.pipeInput(factory.create(Arrays.asList(
                new KeyValue<>(null, "Hello Kafka Streams"),
                new KeyValue<>(null, "All streams lead to Kafka"),
                new KeyValue<>(null, "Join Kafka Summit")
        )));

        Map<String, Long> expectedWordCounts = new HashMap<>();
        expectedWordCounts.put("hello", 1L);
        expectedWordCounts.put("all", 1L);
        expectedWordCounts.put("streams", 2L);
        expectedWordCounts.put("lead", 1L);
        expectedWordCounts.put("to", 1L);
        expectedWordCounts.put("join", 1L);
        expectedWordCounts.put("kafka", 3L);
        expectedWordCounts.put("summit", 1L);

        Map<String, Long> actualWordCounts = new HashMap<>();
        while (true) {
            ProducerRecord<String, Long> outputRecord = testDriver.readOutput(
                    WordCount.OUTPUT_TOPIC,
                    new StringDeserializer(),
                    new LongDeserializer()
            );
            if (outputRecord == null) {
                break;
            }
            actualWordCounts.put(outputRecord.key(), outputRecord.value());
        }
        Assert.assertEquals(expectedWordCounts, actualWordCounts);
    }
}
