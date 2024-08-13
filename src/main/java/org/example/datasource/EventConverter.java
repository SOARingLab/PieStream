package org.example.datasource;

import org.example.events.Attribute;
import org.example.events.PointEvent;
import org.example.events.Schema;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Arrays;

public class EventConverter {
    private final Schema schema;

    public EventConverter(Schema schema) {
        this.schema = schema;
    }

    /**
     * 将原始事件标准化为 PointEvent，不考虑时间戳。
     *
     * @param rawEvent 原始事件
     * @return 标准化后的 PointEvent
     */
    public PointEvent convert(Object rawEvent) {
        // 解析原始事件并构建 payload
        Map<Attribute, Object> payload = parseRawEventToPayload(rawEvent);

        // 直接创建 PointEvent 对象，时间戳可以是默认值（如0）
        return new PointEvent(payload, 0);
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