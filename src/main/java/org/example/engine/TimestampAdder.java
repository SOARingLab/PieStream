package org.example.engine;

import org.example.events.Attribute;
import org.example.events.PointEvent;
import org.example.events.Schema;

import java.util.Map;

public class TimestampAdder {
    private final Schema schema;
    private static long timestampCounter = 0; // 用于递增时间戳

    public TimestampAdder(Schema schema) {
        this.schema = schema;
    }

    /**
     * 为没有时间戳的 PointEvent 增加时间戳。
     *
     * @param pointEvent 已经标准化的 PointEvent
     * @return 增加时间戳后的 PointEvent
     */
    public PointEvent addTimestampIfAbsent(PointEvent pointEvent) {
        long timestamp;

        // 如果 Schema 中有原生时间戳字段，则使用原生时间戳
        if (schema.hasNativeTimestamp()) {
            timestamp = extractTimestampFromPayload(pointEvent.getPayload());
        } else {
            // 如果没有原生时间戳，则分配递增时间戳
            synchronized (TimestampAdder.class) {
                timestamp = timestampCounter++;
            }
        }

        return new PointEvent(pointEvent.getPayload(), timestamp);
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
}