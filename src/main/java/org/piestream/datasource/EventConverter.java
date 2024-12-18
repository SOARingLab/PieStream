package org.piestream.datasource;

import org.piestream.events.Attribute;
import org.piestream.events.PointEvent;
import org.piestream.parser.Schema;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Arrays;

/**
 * Utility class for converting raw event data into standardized {@link PointEvent} objects.
 * The class provides methods for handling raw event data in various formats (e.g., Map, List, String),
 * normalizing them according to a defined schema, and creating corresponding PointEvent objects.
 */
public class EventConverter {
    private final Schema schema;

    /**
     * Constructor that initializes the EventConverter with a specific schema.
     * The schema defines the structure of events and their attributes.
     *
     * @param schema the schema used to map event data to attributes
     */
    public EventConverter(Schema schema) {
        this.schema = schema;
    }

    /**
     * Converts a raw event into a standardized PointEvent.
     * The event is normalized to a Map of attributes and values, and a PointEvent is created.
     * The timestamp is set to the default value (0).
     *
     * @param rawEvent the raw event data (could be in various formats such as String, List, or Map)
     * @return a standardized PointEvent
     */
    public PointEvent convert(Object rawEvent) {
        // Parse the raw event data and build the payload map
        Map<Attribute, Object> payload = parseRawEventToPayload(rawEvent);

        // Create and return a PointEvent with the default timestamp (0)
        return new PointEvent(payload, 0);
    }

    /**
     * Parses the raw event data into a Map representing the payload.
     * The method handles raw events in different formats and maps them to the schema-defined attributes.
     *
     * @param rawEvent the raw event, which could be a String, List, or Map
     * @return a Map representing the parsed event's payload
     * @throws IllegalArgumentException if the raw event format is unsupported
     */
    private Map<Attribute, Object> parseRawEventToPayload(Object rawEvent) {
        Map<Attribute, Object> payload = new HashMap<>();

        if (rawEvent instanceof Map) {
            // If the raw event is already a Map, map it to attributes
            Map<String, Object> rawMap = (Map<String, Object>) rawEvent;
            for (Attribute attribute : schema.getAttributes()) {
                payload.put(attribute, rawMap.get(attribute.getName()));
            }
        } else if (rawEvent instanceof String) {
            // If the raw event is a String, split it into a List
            String rawString = (String) rawEvent;
            List<String> rawList = Arrays.asList(rawString.split(","));
            return convertListToPayload(rawList);
        } else if (rawEvent instanceof List) {
            // If the raw event is a List, directly map it to attributes
            return convertListToPayload((List<?>) rawEvent);
        } else {
            throw new IllegalArgumentException("Unsupported raw event format");
        }

        return payload;
    }

    /**
     * Converts a List of raw event data into a Map of attributes and values.
     * Each value in the List corresponds to an attribute as defined in the schema.
     *
     * @param rawList the raw event represented as a List
     * @return a Map where the keys are attributes, and the values are the corresponding raw event values
     * @throws IllegalArgumentException if the List size does not match the number of attributes in the schema
     */
    private Map<Attribute, Object> convertListToPayload(List<?> rawList) {
        Map<Attribute, Object> payload = new HashMap<>();
        List<Attribute> attributes = schema.getAttributes();

        // Check if the List size matches the schema's field count
        if (rawList.size() != attributes.size()) {
            throw new IllegalArgumentException("List size does not match schema field count");
        }

        // Map the List values to the corresponding schema attributes
        for (int i = 0; i < rawList.size(); i++) {
            Attribute attribute = attributes.get(i);
            payload.put(attribute, rawList.get(i));
        }

        return payload;
    }
}
