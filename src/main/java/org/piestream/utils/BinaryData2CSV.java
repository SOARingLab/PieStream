package org.piestream.utils;

import org.piestream.events.Attribute;
import org.piestream.parser.Schema;

import java.io.*;
import java.util.*;

public class BinaryData2CSV {
    private DataInputStream dataInputStream;
    private Schema schema;
    private List<Attribute> attributes;

    public BinaryData2CSV(String binaryFilePath, Schema schema) throws IOException {
        // Load Schema
        this.schema = schema;
        this.attributes = schema.getAttributes();

        // Initialize DataInputStream
        FileInputStream fileInputStream = new FileInputStream(binaryFilePath);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        this.dataInputStream = new DataInputStream(bufferedInputStream);
    }

    public void convertToCSV(String csvFilePath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFilePath))) {
            // Write CSV header
            List<String> header = new ArrayList<>();
            for (Attribute attribute : attributes) {
                header.add(attribute.getName());
            }
            writer.write(String.join(",", header));
            writer.newLine();

            // Read and write records
            Map<String, Object> recordMap;
            while ((recordMap = readNextRecord()) != null) {
                List<String> recordValues = new ArrayList<>();
                for (String attributeName : header) {
                    Object value = recordMap.get(attributeName);
                    if (value instanceof String) {
                        // Escape double quotes in strings
                        String escapedValue = ((String) value).replace("\"", "\"\"");
                        recordValues.add("\"" + escapedValue + "\"");
                    } else {
                        recordValues.add(String.valueOf(value));
                    }
                }
                writer.write(String.join(",", recordValues));
                writer.newLine();
            }

            System.out.println("Conversion completed. CSV file saved at: " + csvFilePath);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    private Map<String, Object> readNextRecord() {
        try {
            Map<String, Object> recordMap = new LinkedHashMap<>();
            for (Attribute attribute : attributes) {
                String type = attribute.getType().toLowerCase();
                switch (type) {
                    case "byte":
                        recordMap.put(attribute.getName(), dataInputStream.readByte());
                        break;
                    case "short":
                        recordMap.put(attribute.getName(), dataInputStream.readShort());
                        break;
                    case "int":
                    case "integer":
                        recordMap.put(attribute.getName(), dataInputStream.readInt());
                        break;
                    case "long":
                        recordMap.put(attribute.getName(), dataInputStream.readLong());
                        break;
                    case "float":
                        recordMap.put(attribute.getName(), dataInputStream.readFloat());
                        break;
                    case "double":
                        recordMap.put(attribute.getName(), dataInputStream.readDouble());
                        break;
                    case "string":
                        recordMap.put(attribute.getName(), dataInputStream.readUTF());
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown data type: " + attribute.getType());
                }
            }
            return recordMap;
        } catch (EOFException e) {
            // Reached the end of the file
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void close() {
        try {
            dataInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Main method to run the conversion
    public static void main(String[] args) {
        String binaryFilePath = "src/main/resources/data/linear_accel_filtered_404.events";
        String schemaFilePath = "src/main/resources/domain/linearBin_404.yaml";
        String csvFilePath = "src/main/resources/data/linear_accel_filtered_404.csv";

        try {
            // Load Schema
            Schema schema = new Schema(schemaFilePath);

            // Create BinaryData2CSV instance
            BinaryData2CSV converter = new BinaryData2CSV(binaryFilePath, schema);

            // Convert to CSV
            converter.convertToCSV(csvFilePath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
