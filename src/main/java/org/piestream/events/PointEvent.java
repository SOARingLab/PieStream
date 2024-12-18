package org.piestream.events;

import java.util.HashMap;
import java.util.Map;

public class PointEvent {

    // Map to store the event's data as key-value pairs, where the key is an Attribute and the value can be any Object
    private final Map<Attribute, Object> payload;

    // Timestamp of the event indicating when it occurred
    private final long timestamp;

    /**
     * Constructs a PointEvent with a given payload and timestamp.
     *
     * @param payload A map containing the event's data (key-value pairs)
     * @param timestamp The timestamp indicating when the event occurred
     */
    public PointEvent(Map<Attribute, Object> payload, long timestamp) {
        this.payload = payload;
        this.timestamp = timestamp;
    }

    /**
     * Copy constructor to create a new PointEvent by copying data from another PointEvent.
     * This ensures a deep copy of the payload map.
     *
     * @param other The PointEvent instance to copy from
     */
    public PointEvent(PointEvent other) {
        this.timestamp = other.timestamp;

        // Deep copy of the payload map
        this.payload = new HashMap<>();
        for (Map.Entry<Attribute, Object> entry : other.payload.entrySet()) {
            Attribute copiedAttribute = entry.getKey();
            Object copiedValue = entry.getValue();
            this.payload.put(copiedAttribute, copiedValue);
        }
    }

    /**
     * Retrieves the payload of the event.
     * The payload is a map of attributes and their corresponding values.
     *
     * @return The payload of the event as a Map of Attribute-Object pairs
     */
    public Map<Attribute, Object> getPayload() {
        return payload;
    }

    /**
     * Retrieves the timestamp of the event.
     *
     * @return The timestamp of the event
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns a string representation of the PointEvent, including the timestamp and the payload.
     * The string is formatted as "PointEvent { timestamp=..., payload={...} }".
     *
     * @return A string representation of the PointEvent
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PointEvent { ");
        sb.append("timestamp=").append(timestamp).append(", ");
        sb.append("payload={");

        // Iterate over the payload map to append each attribute and its value
        for (Map.Entry<Attribute, Object> entry : payload.entrySet()) {
            sb.append(entry.getKey().getName()).append(": ").append(entry.getValue()).append(", ");
        }

        // Remove the trailing comma and space if the payload is not empty
        if (!payload.isEmpty()) {
            sb.setLength(sb.length() - 2);
        }

        sb.append("} }");
        return sb.toString();
    }
}
