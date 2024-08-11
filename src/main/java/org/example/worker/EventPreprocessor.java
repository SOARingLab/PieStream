package org.example.worker;

import org.example.events.Attribute;
import org.example.events.PointEvent;
import org.example.events.Schema;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Arrays;

public class EventPreprocessor {
    private final Schema schema;
    private static long timestampCounter = 0; // 用于递增时间戳

    public EventPreprocessor(Schema schema) {
        this.schema = schema;
    }

    /**
     * 标准化单个事件，并为缺少时间戳的事件分配时间戳。
     *
     * @param rawEvent 原始事件
     * @return 标准化后的 PointEvent
     */
    public PointEvent preprocess(Object rawEvent) {
        // 解析原始事件并构建 payload
        Map<Attribute, Object> payload = parseRawEventToPayload(rawEvent);

        long timestamp;
        // 如果 Schema 中有原生时间戳字段，则使用原生时间戳
        if (schema.hasNativeTimestamp()) {
            timestamp = extractTimestampFromPayload(payload);
        } else {
            // 如果没有原生时间戳，则分配递增时间戳
            synchronized (EventPreprocessor.class) {
                timestamp = timestampCounter++;
            }
        }

        return new PointEvent(payload, timestamp);
    }

    /**
     * 从 payload 中提取时间戳。
     *
     * @param payload 事件的 payload
     * @return 提取出的时间戳
     * @throws IllegalArgumentException 如果找不到时间戳字段
     */
    private long extractTimestampFromPayload(Map<Attribute, Object> payload) {
        // 找到 timestampField 对应的 Attribute
        Attribute timestampAttribute = schema.getAttributes().stream()
                .filter(attr -> attr.getName().equals(schema.getTimestampField()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Timestamp field not found in schema"));

        Object timestampObj = payload.get(timestampAttribute);
        if (timestampObj == null) {
            throw new IllegalArgumentException("Timestamp field not found in event payload");
        }
        return Long.parseLong(timestampObj.toString());
    }

    /**
     * 将原始事件转换为 Map 类型的 payload。
     *
     * @param rawEvent 原始事件（可能是 String, List, 或 Map）
     * @return 解析后的 payload
     */
    private Map<Attribute, Object> parseRawEventToPayload(Object rawEvent) {
        Map<Attribute, Object> payload = new HashMap<>();

        if (rawEvent instanceof Map) {
            // 如果已经是 Map 类型，将其转换为 Map<Attribute, Object>
            Map<String, Object> rawMap = (Map<String, Object>) rawEvent;
            for (Attribute attribute : schema.getAttributes()) {
                payload.put(attribute, rawMap.get(attribute.getName()));
            }
        } else if (rawEvent instanceof String) {
            // 如果是字符串，解析为 List
            String rawString = (String) rawEvent;
            List<String> rawList = Arrays.asList(rawString.split(","));
            return convertListToPayload(rawList);
        } else if (rawEvent instanceof List) {
            // 如果是 List，直接转换为 Map<Attribute, Object>
            return convertListToPayload((List<?>) rawEvent);
        } else {
            throw new IllegalArgumentException("Unsupported raw event format");
        }

        return payload;
    }

    /**
     * 将 List 转换为 Map<Attribute, Object>，使用 Schema 中定义的字段名作为键
     *
     * @param rawList 事件的 List 表示
     * @return 转换后的 Map
     */
    private Map<Attribute, Object> convertListToPayload(List<?> rawList) {
        Map<Attribute, Object> payload = new HashMap<>();
        List<Attribute> attributes = schema.getAttributes();
        if (rawList.size() != attributes.size()) {
            throw new IllegalArgumentException("List size does not match schema field count");
        }
        for (int i = 0; i < rawList.size(); i++) {
            Attribute attribute = attributes.get(i);
            payload.put(attribute, rawList.get(i));
        }
        return payload;
    }
}
