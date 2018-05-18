package sandbox.kafka_streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class TestUtils {
    public static KafkaStreams createTopicPrinter(String topic,
                                                  String applicationIdConfig,
                                                  String bootstrapServersConfig,
                                                  String defaultKeySerdeClassConfig,
                                                  String defaultValueSerdeClassConfig) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(topic).foreach((key, value) ->
                System.out.println("[" + applicationIdConfig + "] " + key + ": " + value));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationIdConfig);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, defaultKeySerdeClassConfig);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, defaultValueSerdeClassConfig);

        return new KafkaStreams(builder.build(), props);
    }
}
