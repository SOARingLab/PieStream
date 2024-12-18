package org.piestream.engine;


import org.piestream.events.Attribute;
import org.piestream.events.PointEvent;
import org.piestream.events.PointEventIterator;
import org.piestream.parser.Schema;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Arrays;

/**
 * EventPreprocessor class handles the preprocessing of raw event data into standardized PointEvent objects.
 * It provides functionality to parse raw data in different formats (JSON, CSV, BIN) into a structured format
 * based on the provided schema and assigns timestamps to the events.
 */
public class EventPreprocessor {
    private final Schema schema;  // Schema defining the structure and attributes of the events
    private static long timestampCounter = 1;  // Counter for generating unique timestamps for events
    private static final ObjectMapper objectMapper = new ObjectMapper();  // JSON parser for processing raw JSON data
    private final PointEventIterator pointEventIterator;  // Iterator for processing point events
    private final Map<Attribute, Object> reusePayload = new HashMap<>();  // Reusable map for event payloads

    /**
     * Constructor to initialize the EventPreprocessor with the provided schema.
     *
     * @param schema The schema that defines the structure and attributes of the events
     */
    public EventPreprocessor(Schema schema) {
        this.schema = schema;
        this.pointEventIterator = new PointEventIterator();  // Initialize the point event iterator
    }

    /**
     * Standardizes a single raw event and assigns a timestamp to the event if not already provided.
     * This method assumes the event has a timestamp field defined by the schema.
     *
     * @param rawPointEvent The raw event as a string (e.g., JSON, CSV, or BIN)
     * @return A standardized PointEvent with the corresponding timestamp
     */
    public PointEvent preprocess(String rawPointEvent) {
        return preprocess(rawPointEvent, true);  // Default to use native timestamp
    }

    /**
     * Standardizes a single raw event with the option to use a native timestamp.
     *
     * @param rawPointEvent The raw event data as a string (JSON, CSV, BIN)
     * @param useNativeTimestamp Flag indicating whether to use a native timestamp from the raw event
     * @return A standardized PointEvent with a timestamp
     */
    public PointEvent preprocess(String rawPointEvent, boolean useNativeTimestamp) {
        // Parse the raw event data into a structured payload
        Map<Attribute, Object> payload = parseRawEventToPayload(rawPointEvent);

        long timestamp;
        if (useNativeTimestamp && schema.hasNativeTimestamp()) {
            // Extract the timestamp if a native timestamp is provided in the raw event
            timestamp = extractTimestampFromPayload(payload);
        } else {
            // If no native timestamp is available, assign an incrementing timestamp
            synchronized (EventPreprocessor.class) {
                timestamp = timestampCounter++;  // Ensure thread safety for timestamp counter
            }
        }

        // Return the standardized PointEvent with the extracted/assigned timestamp
        return new PointEvent(payload, timestamp);
    }

    /**
     * Extracts the timestamp from the event's payload.
     * The timestamp is assumed to be in the field defined by the schema's timestamp attribute.
     *
     * @param payload The event's payload (a map of attributes and their values)
     * @return The extracted timestamp value
     * @throws IllegalArgumentException If the timestamp field is missing or invalid
     */
    private long extractTimestampFromPayload(Map<Attribute, Object> payload) {
        // Find the timestamp attribute based on the schema
        Attribute timestampAttribute = schema.getAttributes().stream()
                .filter(attr -> attr.getName().equals(schema.getTimestampField()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Timestamp field not found in schema"));

        // Get the timestamp value from the payload
        Object timestampObj = payload.get(timestampAttribute);
        if (timestampObj == null) {
            throw new IllegalArgumentException("Timestamp field not found in event payload");
        }

        // If the timestamp attribute is of type "long", convert it accordingly
        if ("long".equalsIgnoreCase(timestampAttribute.getType())) {
            if (timestampObj instanceof Long) {
                return (Long) timestampObj;  // Direct conversion if the timestamp is already a Long
            } else if (timestampObj instanceof String) {
                try {
                    return Long.parseLong((String) timestampObj);  // Parse if the timestamp is a string
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid long value: " + timestampObj, e);
                }
            }
        }

        // If it's neither Long nor String, convert to Long after calling toString
        return Long.parseLong(timestampObj.toString());
    }

    /**
     * Parses a raw event (in various formats like JSON, CSV, BIN) into a Map of attributes and their corresponding values.
     *
     * @param rawEvent The raw event data, which can be a String, List, or Map
     * @return A map containing the attributes and their corresponding values
     * @throws IllegalArgumentException If the raw event format is unsupported or invalid
     */
    private Map<Attribute, Object> parseRawEventToPayload(Object rawEvent) {
        Map<Attribute, Object> payload = new HashMap<>();

        String rawdataType = schema.getRawdataType();  // Get the raw data type (JSON, CSV, BIN)

        // Handle JSON data format
        if ("JSON".equalsIgnoreCase(rawdataType)) {
            if (rawEvent instanceof String) {
                try {
                    // Parse the JSON string into a Map of attributes
                    Map<String, Object> rawMap = objectMapper.readValue((String) rawEvent, Map.class);
                    for (Attribute attribute : schema.getAttributes()) {
                        payload.put(attribute, rawMap.get(attribute.getName()));  // Populate the payload map
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to parse JSON string", e);
                }
            } else {
                throw new IllegalArgumentException("For JSON data, rawEvent should be a JSON formatted string");
            }
        }
        // Handle CSV data format
        else if ("CSV".equalsIgnoreCase(rawdataType)) {
            if (rawEvent instanceof String) {
                String rawString = (String) rawEvent;
                String[] rawArray = rawString.split(",", -1);  // Split CSV by commas, preserving empty strings
                List<String> rawList = Arrays.asList(rawArray);
                return convertListToPayload(rawList);  // Convert the CSV data into a structured payload
            } else if (rawEvent instanceof List) {
                return convertListToPayload((List<?>) rawEvent);  // Convert from List format
            } else {
                throw new IllegalArgumentException("For CSV data, rawEvent should be of type String or List");
            }
        }
        // Handle BIN data format
        else if ("BIN".equalsIgnoreCase(rawdataType)) {
            if (rawEvent instanceof String) {
                try {
                    Map<String, Object> rawMap = objectMapper.readValue((String) rawEvent, Map.class);
                    for (Attribute attribute : schema.getAttributes()) {
                        payload.put(attribute, rawMap.get(attribute.getName()));
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to parse BIN data", e);
                }
            } else {
                throw new IllegalArgumentException("For BIN data, rawEvent should be a BIN formatted string");
            }
        }
        // Throw exception for unsupported data types
        else {
            throw new IllegalArgumentException("Unsupported rawdataType: " + rawdataType);
        }

        return payload;
    }

    /**
     * Converts a List representing an event's fields into a Map of attributes and values.
     * The size of the List must match the number of attributes in the schema.
     *
     * @param rawList The raw event data in List format
     * @return A Map of attributes and their corresponding values
     * @throws IllegalArgumentException If the List size does not match the number of attributes in the schema
     */
    private Map<Attribute, Object> convertListToPayload(List<?> rawList) {
        reusePayload.clear();  // Clear the previous payload

        List<Attribute> attributes = schema.getAttributes();
        if (rawList.size() != attributes.size()) {
            throw new IllegalArgumentException("List size does not match schema field count");
        }

        // Populate the payload map with attribute-value pairs
        for (int i = 0; i < rawList.size(); i++) {
            Attribute attribute = attributes.get(i);
            reusePayload.put(attribute, rawList.get(i));
        }

        return reusePayload;
    }
}
