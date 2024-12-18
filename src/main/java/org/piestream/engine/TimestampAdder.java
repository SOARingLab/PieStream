package org.piestream.engine;

import org.piestream.events.Attribute;
import org.piestream.events.PointEvent;
import org.piestream.parser.Schema;

import java.util.Map;

/**
 * The TimestampAdder class is responsible for adding a timestamp to a PointEvent if it is absent.
 * It handles two scenarios:
 * 1. If the event payload already contains a timestamp field, it extracts and uses that timestamp.
 * 2. If the event payload does not contain a timestamp, it generates a unique incremental timestamp.
 *
 * The class works with the Schema to determine whether a native timestamp field exists in the event payload
 * and ensures that every PointEvent has a valid timestamp.
 */
public class TimestampAdder {
    private final Schema schema;  // The schema that defines the structure of the PointEvent, including the timestamp field
    private static long timestampCounter = 0;  // A static counter to generate incremental timestamps

    /**
     * Constructor to initialize the TimestampAdder with the provided Schema.
     * The schema defines how the PointEvent should be structured, including the presence of a timestamp field.
     *
     * @param schema The schema used to define the structure of PointEvents, including any timestamp-related information
     */
    public TimestampAdder(Schema schema) {
        this.schema = schema;
    }

    /**
     * Adds a timestamp to a PointEvent if it is not already present.
     *
     * - If the schema has a native timestamp field, the timestamp is extracted from the payload.
     * - If no timestamp is found, an incremental timestamp is generated using a static counter.
     *
     * @param pointEvent The PointEvent object that needs a timestamp
     * @return A new PointEvent object with an added or extracted timestamp
     */
    public PointEvent addTimestampIfAbsent(PointEvent pointEvent) {
        long timestamp;

        // If the schema defines a native timestamp field, use it
        if (schema.hasNativeTimestamp()) {
            timestamp = extractTimestampFromPayload(pointEvent.getPayload());
        } else {
            // Otherwise, generate a unique incremental timestamp
            synchronized (TimestampAdder.class) {
                timestamp = timestampCounter++;
            }
        }

        // Return a new PointEvent with the added timestamp
        return new PointEvent(pointEvent.getPayload(), timestamp);
    }

    /**
     * Extracts the timestamp from the payload of the PointEvent.
     * The timestamp is obtained from the field defined in the schema (timestamp field).
     *
     * If the timestamp field is not found or the timestamp is missing from the payload,
     * an IllegalArgumentException is thrown.
     *
     * @param payload The payload of the PointEvent containing event attributes
     * @return The extracted timestamp value
     * @throws IllegalArgumentException if the timestamp field is not found in the schema or payload
     */
    private long extractTimestampFromPayload(Map<Attribute, Object> payload) {
        // Locate the timestamp field in the schema
        Attribute timestampAttribute = schema.getAttributes().stream()
                .filter(attr -> attr.getName().equals(schema.getTimestampField()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Timestamp field not found in schema"));

        // Retrieve the timestamp value from the payload using the timestamp attribute
        Object timestampObj = payload.get(timestampAttribute);
        if (timestampObj == null) {
            throw new IllegalArgumentException("Timestamp field not found in event payload");
        }

        // Convert the timestamp to a long value
        return Long.parseLong(timestampObj.toString());
    }
}
