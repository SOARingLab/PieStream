package org.piestream.events;

import java.util.Map;

public class PointEvent {
    private final Map<Attribute, Object> payload;
    private final long timestamp;

    public PointEvent(Map<Attribute, Object> payload, long timestamp) {
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public Map<Attribute, Object> getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PointEvent { ");
        sb.append("timestamp=").append(timestamp).append(", ");
        sb.append("payload={");
        for (Map.Entry<Attribute, Object> entry : payload.entrySet()) {
            sb.append(entry.getKey().getName()).append(": ").append(entry.getValue()).append(", ");
//            sb.append(entry.getValue()).append(", ");
        }
        // 删除最后一个逗号和空格
        if (!payload.isEmpty()) {
            sb.setLength(sb.length() - 2);
        }
        sb.append("} }");
        return sb.toString();
    }
}
