package org.piestream.datasource;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.piestream.engine.Engine;
import org.piestream.evaluation.Correct;
import org.piestream.events.Attribute;
import org.piestream.parser.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * This class implements the DataSource interface for reading binary data from a file.
 * It supports reading data based on a given schema and converting it into a JSON string format.
 * It is designed to process event data in a streaming manner, enabling real-time or batch processing of large data files.
 */
public class BinaryDataSource implements DataSource {

    // Logger for tracking information, warnings, and errors
    private static final Logger logger = LoggerFactory.getLogger(BinaryDataSource.class);

    // Stream and schema-related variables
    private DataInputStream dataInputStream;
    private Schema schema;
    private List<Attribute> attributes;
    private boolean hasNextRecord;
    private String nextRecord;
    private ObjectMapper objectMapper; // ObjectMapper for converting data to JSON

    /**
     * Constructor for initializing the BinaryDataSource with a binary file and a schema.
     * The constructor reads the schema and sets up the necessary input streams for reading data.
     *
     * @param binaryFilePath the path to the binary data file
     * @param schema         the schema describing the data structure
     * @throws IOException if there is an error reading the binary file
     */
    public BinaryDataSource(String binaryFilePath, Schema schema) throws IOException {
        // Load the schema and its attributes
        this.schema = schema;
        this.attributes = schema.getAttributes();

        // Initialize the data input stream for reading binary data
        FileInputStream fileInputStream = new FileInputStream(binaryFilePath);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        this.dataInputStream = new DataInputStream(bufferedInputStream);

        // Initialize ObjectMapper for JSON conversion
        this.objectMapper = new ObjectMapper();

        // Pre-read the first record
        this.nextRecord = readNextRecord();
        this.hasNextRecord = this.nextRecord != null;
    }

    /**
     * Reads the next record from the binary file, processes it according to the schema,
     * and converts it into a JSON string.
     * The record's fields are read based on their types defined in the schema.
     *
     * @return the JSON representation of the next record, or null if the end of the file is reached
     */
    private String readNextRecord() {
        try {
            Map<String, Object> recordMap = new LinkedHashMap<>();
            // Process each attribute and read the corresponding data from the stream
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
            // Convert the record map to a JSON string using ObjectMapper
            return objectMapper.writeValueAsString(recordMap);
        } catch (EOFException e) {
            // End of file reached, return null
            return null;
        } catch (IOException e) {
            // Log and return null in case of error reading the record
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Reads the next record from the data source.
     * This method updates the `hasNextRecord` flag based on whether there are more records to read.
     *
     * @return the current record in JSON format
     */
    @Override
    public String readNext() {
        // Get the current record
        String currentRecord = this.nextRecord;

        // Read the next record
        this.nextRecord = readNextRecord();

        // Update the hasNextRecord flag
        this.hasNextRecord = this.nextRecord != null;

        return currentRecord;
    }

    /**
     * Checks if there are more records to read.
     *
     * @return true if there are more records, false otherwise
     */
    @Override
    public boolean hasNext() {
        return hasNextRecord;
    }

    /**
     * Reads a batch of records from the data source.
     * This method reads the specified number of records in a batch and returns them as a list.
     *
     * @param batchSize the number of records to read
     * @return a list of records in JSON format
     */
    @Override
    public List<String> readBatch(int batchSize) {
        List<String> batch = new ArrayList<>();
        for (int i = 0; i < batchSize && hasNext(); i++) {
            batch.add(readNext());
        }
        return batch;
    }

    /**
     * Closes the data input stream and releases any associated resources.
     */
    @Override
    public void close() {
        try {
            dataInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Main method for testing the BinaryDataSource class.
     * This method loads a schema, creates an engine instance, and processes records from a binary data file.
     *
     * @param args command line arguments (not used)
     */
    public static void main(String[] args) {
        String binaryFilePath = "/Users/czq/Code/TPS_data/linear_accel_filtered_404.events";
        String schemaFilePath = "src/main/resources/domain/linearBin_404.yaml";
        Schema schema = new Schema(schemaFilePath); // Load the schema

        String query =
                "FROM CarStream " +
                        "DEFINE D AS ACCEL <= -0.00455 , S AS SPEED >= 32 , A AS ACCEL >= 0.0050 " +
                        "PATTERN " +
                        "A  meets; overlaps; starts; during ; before    S  " +
                        "AND S  meets; contains; followed-by; overlaps;after;before   D  " +
                        "WITHIN 1000000 "+
                        "RETURN s.ts, s.te ";

        // Create an Engine instance
        Engine engine = new Engine(schema, query);

        try (DataSource dataSource = new BinaryDataSource(binaryFilePath, schema)) {
            long time = -System.nanoTime();
            // Process each record in the data source
            while (dataSource.hasNext()) {
                String record = dataSource.readNext();
                engine.apply("", record); // Process each record
            }

            time += System.nanoTime();
            engine.printResultCNT(); // Print the result count

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
