package org.piestream.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class Config {
    private String rawdataType;
    private String timestamp;
    private List<Field> fields;

    // Getter and Setter for rawdataType
    public String getRawdataType() {
        return rawdataType;
    }

    public void setRawdataType(String rawdataType) {
        this.rawdataType = rawdataType;
    }

    // Getter and Setter for timestamp
    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    // Getter and Setter for fields
    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    // Inner class Field representing each field in the schema
    public static class Field {
        private String name;
        private String type;

        // Getter and Setter for name
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        // Getter and Setter for type
        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    // Method to load the config from a YAML file
    public static Config loadConfig(String filePath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        return objectMapper.readValue(new File(filePath), Config.class);
    }

    // Main method to test the config loading
    public static void main(String[] args) {
        try {
            String configFilePath = "src/main/resources/domain/linear_accel.yaml"; // Replace with your config file path
            Config config = Config.loadConfig(configFilePath);

            // Output the rawdataType
            System.out.println("Raw Data Type: " + config.getRawdataType());

            // Output the timestamp field
            System.out.println("Timestamp field: " + config.getTimestamp());

            // Output the field information
            for (Field field : config.getFields()) {
                System.out.println("Field name: " + field.getName() + ", type: " + field.getType());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
