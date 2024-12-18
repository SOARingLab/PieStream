package org.piestream.datasource;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

/**
 * KafkaTransformer is a custom implementation of the Kafka Streams Transformer interface.
 * It processes incoming records from a Kafka stream and applies a two-stage transformation
 * to the values before emitting the result to an output stream.
 */
public class KafkaTransformer implements Transformer<String, String, KeyValue<String, String>> {

    private ProcessorContext context; // ProcessorContext is used to interact with the stream's state and configuration

    /**
     * Initializes the transformer with the given ProcessorContext. This method is called once
     * when the transformer is added to the Kafka Streams topology.
     *
     * @param context the ProcessorContext
     */
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        // If state stores are required, they can be initialized here
        // e.g., context.getStateStore("my-store");
    }

    /**
     * Transforms the input key-value pair by applying a two-stage processing logic.
     *
     * @param key   the input key
     * @param value the input value
     * @return a KeyValue pair with the transformed key and value
     */
    @Override
    public KeyValue<String, String> transform(String key, String value) {
        // Stage 1: Perform the first transformation step
        String stage1Result = performStage1(value);

        // Stage 2: Perform the second transformation step
        String stage2Result = performStage2(stage1Result);

        // Return the transformed key-value pair
        return new KeyValue<>(key, stage2Result);
    }

    /**
     * Releases any resources held by the transformer when it is no longer needed.
     */
    @Override
    public void close() {
        // Resources can be cleaned up here if needed
    }

    /**
     * Performs the first stage of the transformation by converting the value to uppercase.
     *
     * @param value the input value
     * @return the transformed value (uppercase)
     */
    private String performStage1(String value) {
        // Stage 1 transformation logic: Convert value to uppercase
        return value.toUpperCase();
    }

    /**
     * Performs the second stage of the transformation by appending a prefix to the value.
     *
     * @param value the input value
     * @return the transformed value with a prefix
     */
    private String performStage2(String value) {
        // Stage 2 transformation logic: Prefix the value
        return "Processed: " + value;
    }

    /**
     * Main method to configure and start the Kafka Streams application.
     * It builds the stream topology, processes the data, and sends the results to an output topic.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        // Set up Kafka Streams configuration properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-transformer-piestream"); // Unique application ID
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker address
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Key serde (String)
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Value serde (String)

        String inputTopic = "linear-tt"; // Input Kafka topic for stream processing

        // Build the stream processing topology
        StreamsBuilder builder = new StreamsBuilder();

        // Read data from the input topic
        KStream<String, String> inputStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        // Apply the custom KafkaTransformer to the stream
        KStream<String, String> processedStream = inputStream.transform(() -> new KafkaTransformer());

        // Write the processed data to the output topic
        processedStream.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        // Create and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add a shutdown hook to gracefully close the application when the JVM exits
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
