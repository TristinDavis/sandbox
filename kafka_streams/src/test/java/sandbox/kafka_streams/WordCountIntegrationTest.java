package sandbox.kafka_streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class WordCountIntegrationTest {

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.start();
        CLUSTER.createTopic(WordCount.INPUT_TOPIC);
        CLUSTER.createTopic(WordCount.OUTPUT_TOPIC);
        TestUtils.createTopicPrinter(
                WordCount.INPUT_TOPIC,
                "wordcount-integration-test-input-printer",
                CLUSTER.bootstrapServers(),
                Serdes.String().getClass().getName(),
                Serdes.String().getClass().getName()
        ).start();
        TestUtils.createTopicPrinter(
                WordCount.OUTPUT_TOPIC,
                "wordcount-integration-test-output-printer",
                CLUSTER.bootstrapServers(),
                Serdes.String().getClass().getName(),
                Serdes.Long().getClass().getName()
        ).start();
    }

    @Test
    public void shouldCountWords() throws Exception {
        List<String> inputValues = Arrays.asList(
                "Hello Kafka Streams",
                "All streams lead to Kafka",
                "Join Kafka Summit"
        );
        List<KeyValue<String, Long>> expectedWordCounts = Arrays.asList(
                new KeyValue<>("hello", 1L),
                new KeyValue<>("all", 1L),
                new KeyValue<>("streams", 2L),
                new KeyValue<>("lead", 1L),
                new KeyValue<>("to", 1L),
                new KeyValue<>("join", 1L),
                new KeyValue<>("kafka", 3L),
                new KeyValue<>("summit", 1L)
        );

        //
        // Step 1: Configure and start the processor topology.
        //
        Topology topology = WordCount.createTopology();
        Properties props = WordCount.createProps();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.STATE_DIR_CONFIG, org.apache.kafka.test.TestUtils.tempDirectory().getAbsolutePath());

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        IntegrationTestUtils.produceValuesSynchronously(WordCount.INPUT_TOPIC, inputValues, producerConfig, Time.SYSTEM);

        //
        // Step 3: Verify the application's output data.
        //
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "wordcount-integration-test-standard-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        List<KeyValue<String, Long>> actualWordCounts = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig,
                WordCount.OUTPUT_TOPIC, expectedWordCounts.size());
        streams.close();
        assertThat(actualWordCounts).containsExactlyElementsOf(expectedWordCounts);
    }
}
