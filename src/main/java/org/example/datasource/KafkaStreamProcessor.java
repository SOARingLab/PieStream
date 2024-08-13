package org.example.datasource;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.example.engine.Engine;
import java.util.Properties;
import org.example.events.Schema;
import org.apache.kafka.streams.kstream.ForeachAction;

public class KafkaStreamProcessor {

    private final KafkaStreams streams;

    /**
     * 构造函数，初始化 Kafka Stream 处理器
     *
     * @param bootstrapServers Kafka broker 地址
     * @param applicationId    Kafka Streams 应用程序 ID
     * @param inputTopic       输入的 Kafka 主题
     * @param engine           用于处理每条记录的处理函数
     */
    public KafkaStreamProcessor(String bootstrapServers, String applicationId, String inputTopic, ForeachAction<String, String> engine) {
        // 配置 Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // 构建流拓扑
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(inputTopic);

        // 处理流中的每条记录，使用提供的 Engine 处理函数
        stream.foreach(engine);

        // 初始化 KafkaStreams 实例
        streams = new KafkaStreams(builder.build(), props);
    }

    /**
     * 启动流处理
     */
    public void start() {
        streams.start();
    }

    /**
     * 关闭流处理
     */
    public void stop() {
        streams.close();
    }

    /**
     * 添加 JVM 关闭钩子，以便在应用程序终止时正确关闭 Kafka Streams
     */
    public void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

}