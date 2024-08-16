package org.example.datasource;
import org.apache.kafka.streams.KeyValue;

import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.example.engine.Engine;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.common.serialization.Serdes;
import java.util.Properties;
public class KafkaTransformer implements Transformer<String, String, KeyValue<String, String>> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        // 如果需要使用状态存储，可以在这里初始化
        // context.getStateStore("my-store");
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        // 多阶段处理逻辑
        String stage1Result = performStage1(value);
        String stage2Result = performStage2(stage1Result);

        // 返回新的键值对
        return new KeyValue<>(key, stage2Result);
    }

    @Override
    public void close() {
        // 释放资源
    }

    private String performStage1(String value) {
        // 第一阶段处理逻辑
        return value.toUpperCase();
    }

    private String performStage2(String value) {
        // 第二阶段处理逻辑
        return "Processed: " + value;
    }

    public static void main(String[] args) {
        // 配置 Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-transformer-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        String inputTopic = "linear-tt"; // 输入的 Kafka 主题
        // 构建拓扑
        StreamsBuilder builder = new StreamsBuilder();

        // 从输入主题读取数据
        KStream<String, String> inputStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        // 使用自定义的 KafkaTransformer 进行处理
        KStream<String, String> processedStream = inputStream.transform(() -> new KafkaTransformer());

        // 将处理后的数据写入输出主题
        processedStream.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        // 创建和启动 Kafka Streams 应用
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // 添加关闭钩子，优雅地停止应用
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
