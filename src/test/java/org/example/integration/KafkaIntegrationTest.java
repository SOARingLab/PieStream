package org.example.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.datasource.DataSource;
import org.example.factory.DataSourceFactory;
import org.example.worker.EventPreprocessor;
import org.example.events.Schema;
import org.example.events.PointEvent;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaIntegrationTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testConsumeAndProcessKafkaData() {
        String topic = "linear";  // 替换为你的 Kafka 主题
        Schema schema = new Schema("src/test/resources/domain/linear_accel.yaml");  // 替换为你的 Schema 文件路径

        // 配置 Kafka 消费者的属性
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // 从最早的消息开始消费

        // 创建 Kafka 消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        EventPreprocessor eventPreprocessor = new EventPreprocessor(schema);

        // 持续消费 Kafka 主题中的数据
        try {
            System.out.println("Waiting for messages from Kafka topic: " + topic);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                for (ConsumerRecord<String, String> record : records) {
                    // 立即输出接收到的原始消息
                    String value = record.value();
                    System.out.println("Received raw message: " + value);

                    // 将接收到的 JSON 字符串解析为 Map
                    Map<String, Object> rawEvent;
                    try {
                        rawEvent = objectMapper.readValue(value, Map.class);
                    } catch (Exception e) {
                        System.err.println("Failed to parse JSON message: " + e.getMessage());
                        continue;
                    }

                    // 使用 EventPreprocessor 处理数据
                    PointEvent pointEvent = eventPreprocessor.preprocess(rawEvent);

                    // 按格式输出处理后的 PointEvent
                    System.out.println(pointEvent.toString());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
