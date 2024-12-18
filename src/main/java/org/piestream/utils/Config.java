package org.piestream.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * The Config class represents the configuration for the PIEStream framework.
 * It provides methods to load schema data from a YAML file
 * and store various schema parameters such as raw data type, timestamp,
 * timestamp unit, and field definitions.
 */
public class Config {

    private static final Logger logger = LoggerFactory.getLogger(Config.class);

    private String rawdataType;         /* Type of raw data */
    private String timestamp;           /* Timestamp field name */
    private String timestampUnit;       /* Unit of the timestamp (e.g., seconds, milliseconds) */
    private List<Field> fields;         /* List of fields that define the data schema */

    /**
     * Retrieves the type of raw data.
     *
     * @return the type of raw data (e.g., "CSV", "JSON").
     */
    public String getRawdataType() {
        return rawdataType;
    }

    /**
     * Sets the type of raw data.
     *
     * @param rawdataType the type of raw data (e.g., "CSV", "JSON").
     */
    public void setRawdataType(String rawdataType) {
        this.rawdataType = rawdataType;
    }

    /**
     * Retrieves the name of the timestamp field.
     *
     * @return the name of the timestamp field.
     */
    public String getTimestamp() {
        return timestamp;
    }

    /**
     * Sets the name of the timestamp field.
     *
     * @param timestamp the name of the timestamp field.
     */
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Retrieves the unit of the timestamp.
     *
     * @return the unit of the timestamp (e.g., "milliseconds").
     */
    public String getTimestampUnit() {
        return timestampUnit;
    }

    /**
     * Sets the unit of the timestamp.
     *
     * @param timestampUnit the unit of the timestamp (e.g., "milliseconds").
     */
    public void setTimestampUnit(String timestampUnit) {
        this.timestampUnit = timestampUnit;
    }

    /**
     * Retrieves the list of fields that define the data schema.
     *
     * @return the list of fields that define the data schema.
     */
    public List<Field> getFields() {
        return fields;
    }

    /**
     * Sets the list of fields that define the data schema.
     *
     * @param fields the list of fields that define the data schema.
     */
    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    /**
     * The Field class represents each field in the data schema.
     * It contains the name and type of the field.
     */
    public static class Field {
        private String name;   /* Field name */
        private String type;   /* Field type (e.g., "string", "integer") */

        /**
         * Retrieves the name of the field.
         *
         * @return the name of the field.
         */
        public String getName() {
            return name;
        }

        /**
         * Sets the name of the field.
         *
         * @param name the name of the field.
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * Retrieves the type of the field.
         *
         * @return the type of the field.
         */
        public String getType() {
            return type;
        }

        /**
         * Sets the type of the field.
         *
         * @param type the type of the field (e.g., "string", "integer").
         */
        public void setType(String type) {
            this.type = type;
        }
    }

    /**
     * Loads the configuration from a YAML file.
     * Uses Jackson's ObjectMapper to deserialize the YAML file into a Config object.
     *
     * @param filePath the path to the YAML configuration file.
     * @return a Config object representing the configuration.
     * @throws IOException if an I/O error occurs while reading the file.
     */
    public static Config loadConfig(String filePath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        return objectMapper.readValue(new File(filePath), Config.class);
    }
}
