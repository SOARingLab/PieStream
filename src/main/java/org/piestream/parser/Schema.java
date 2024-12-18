package org.piestream.parser;

import org.piestream.events.Attribute;
import org.piestream.utils.Config;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

/**
 * This class represents the schema configuration for a data stream.
 * It includes details such as the raw data type, timestamp field, and a list of attributes (fields) for the data.
 * The schema also handles timestamp units and provides methods to retrieve or manipulate schema-related information.
 */
public class Schema {
    private String rawdataType;  // The type of raw data
    private String timestampField;  // The name of the timestamp field
    private Map<String, String> fields;  // A map of field names to their data types
    private boolean hasNativeTimestamp;  // Flag indicating if the schema has a native timestamp field
    private List<Attribute> attributes;  // A list of attributes (fields) in the schema
    private Map<String, Integer> fieldIndexMap;  // A map of field names to their index in the attribute list
    private TimestampUnit timestampUnit = TimestampUnit.S; // Default timestamp unit is SECOND


    /**
     * Enumeration for timestamp units.
     * It supports SECOND (s), MILLISECOND (ms), MICROSECOND (us), and NANOSECOND (ns).
     */
    public enum TimestampUnit {
        S,
        MS,
        US,
        NS;

        @Override
        public String toString() {
            switch (this) {
                case S: return "s";
                case MS: return "ms";
                case US: return "us";
                case NS: return "ns";
                default: throw new IllegalStateException("Unexpected value: " + this);
            }
        }

        /**
         * Returns the number of nanoseconds that correspond to this timestamp unit.
         *
         * @return the number of nanoseconds per unit.
         */
        public long getNanosPerUnit() {
            switch (this) {
                case S: return 1000000000L;  // 1 second = 1 billion nanoseconds
                case MS: return 1000000L;   // 1 millisecond = 1 million nanoseconds
                case US: return 1000L;      // 1 microsecond = 1 thousand nanoseconds
                case NS: return 1L;         // 1 nanosecond = 1 nanosecond
                default: throw new IllegalStateException("Unexpected value: " + this);
            }
        }

        /**
         * Converts a string representation of a timestamp unit to its corresponding TimestampUnit enum.
         *
         * @param unit the string representation of the timestamp unit (e.g., "s", "ms", "us", "ns").
         * @return the corresponding TimestampUnit enum.
         * @throws IllegalArgumentException if the unit string is unknown.
         */
        public static TimestampUnit fromString(String unit) {
            switch (unit.toLowerCase()) {
                case "s":
                case "S":  return S;
                case "MS":
                case "ms": return MS;
                case "US":
                case "us": return US;
                case "NS":
                case "ns": return NS;
                default: throw new IllegalArgumentException("Unknown timestamp unit: " + unit);
            }
        }
    }

    /**
     * Constructor that loads the schema from a configuration file.
     *
     * @param schemaFilePath the path to the schema configuration file.
     */
    public Schema(String schemaFilePath) {
        this.fields = new HashMap<>();
        this.attributes = new ArrayList<>();
        this.fieldIndexMap = new HashMap<>();
        loadSchema(schemaFilePath);
    }

    /**
     * Constructor for creating a schema with the raw data type, timestamp field, timestamp unit, and a list of attributes.
     *
     * @param rawdataType the type of raw data.
     * @param timestampField the name of the timestamp field.
     * @param timestampUnit the timestamp unit as a string (e.g., "s", "ms", "us", "ns").
     * @param attributes a list of attributes (fields) for the schema.
     */
    public Schema(String rawdataType, String timestampField, String timestampUnit, List<Attribute> attributes) {
        this.rawdataType = rawdataType;
        this.timestampField = timestampField;
        this.hasNativeTimestamp = timestampField != null && !timestampField.isEmpty();
        this.fields = new HashMap<>();
        this.attributes = attributes != null ? new ArrayList<>(attributes) : new ArrayList<>();
        this.fieldIndexMap = new HashMap<>();
        this.timestampUnit = TimestampUnit.fromString(timestampUnit);

        // Initialize fields and fieldIndexMap
        if (attributes != null) {
            for (int i = 0; i < attributes.size(); i++) {
                Attribute attribute = attributes.get(i);
                fields.put(attribute.getName(), attribute.getType());
                fieldIndexMap.put(attribute.getName(), i);
            }
        }
    }

    /**
     * Constructor for creating a schema with the raw data type, timestamp field, and a list of attributes.
     *
     * @param rawdataType the type of raw data.
     * @param timestampField the name of the timestamp field (can be null if not available).
     * @param attributes a list of attributes (fields) for the schema.
     */
    public Schema(String rawdataType, String timestampField, List<Attribute> attributes) {
        this.rawdataType = rawdataType;
        this.timestampField = timestampField;
        this.hasNativeTimestamp = timestampField != null && !timestampField.isEmpty();
        this.fields = new HashMap<>();
        this.attributes = attributes != null ? new ArrayList<>(attributes) : new ArrayList<>();
        this.fieldIndexMap = new HashMap<>();

        // Initialize fields and fieldIndexMap
        if (attributes != null) {
            for (int i = 0; i < attributes.size(); i++) {
                Attribute attribute = attributes.get(i);
                fields.put(attribute.getName(), attribute.getType());
                fieldIndexMap.put(attribute.getName(), i);
            }
        }
    }

    /**
     * Constructor for creating a schema with the raw data type and a list of attributes.
     *
     * @param rawdataType the type of raw data.
     * @param attributes a list of attributes (fields) for the schema.
     */
    public Schema(String rawdataType, List<Attribute> attributes) {
        this(rawdataType, null, attributes);
    }

    /**
     * Loads the schema configuration from a file and initializes the schema attributes.
     *
     * @param schemaFilePath the path to the schema configuration file.
     */
    private void loadSchema(String schemaFilePath) {
        try {
            Config config = Config.loadConfig(schemaFilePath);
            this.rawdataType = config.getRawdataType(); // Get the raw data type
            this.timestampField = config.getTimestamp();
            this.hasNativeTimestamp = timestampField != null && !timestampField.isEmpty();
            this.fields.clear();
            this.attributes.clear();
            this.fieldIndexMap.clear();
            String unit = config.getTimestampUnit();
            if (unit != null) {
                this.timestampUnit = TimestampUnit.fromString(unit);
            } else {
                this.timestampUnit = TimestampUnit.S;  // Default to SECOND if no unit is specified
            }

            // Populate fields, attributes, and fieldIndexMap from the configuration
            for (Config.Field field : config.getFields()) {
                fields.put(field.getName(), field.getType());
                attributes.add(new Attribute(field.getName(), field.getType()));
                fieldIndexMap.put(field.getName(), attributes.size() - 1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets the timestamp unit for the schema.
     *
     * @return the timestamp unit as a TimestampUnit enum.
     */
    public TimestampUnit getTimestampUnit() {
        return timestampUnit;
    }

    /**
     * Gets the raw data type for the schema.
     *
     * @return the raw data type as a string.
     */
    public String getRawdataType() {
        return rawdataType;
    }

    /**
     * Gets the timestamp field for the schema.
     *
     * @return the timestamp field name as a string.
     */
    public String getTimestampField() {
        return timestampField;
    }

    /**
     * Gets the fields of the schema as a map of field names to types.
     *
     * @return a map of field names to their types.
     */
    public Map<String, String> getFields() {
        return fields;
    }

    /**
     * Checks if the schema has a native timestamp field.
     *
     * @return true if the schema has a native timestamp field, false otherwise.
     */
    public boolean hasNativeTimestamp() {
        return hasNativeTimestamp;
    }

    /**
     * Gets the list of attributes (fields) for the schema.
     *
     * @return a list of attributes.
     */
    public List<Attribute> getAttributes() {
        return attributes;
    }

    /**
     * Gets the map of field names to their index in the attribute list.
     *
     * @return a map of field names to their index.
     */
    public Map<String, Integer> getFieldIndexMap() {
        return fieldIndexMap;
    }
}
