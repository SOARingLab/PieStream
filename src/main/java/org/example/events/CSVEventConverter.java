package org.example.events;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CSVEventConverter implements PointEvents {
    private final String csvFilePath;
    private final Schema schema;
    private final List<PointEvent> events;

    public CSVEventConverter(String csvFilePath, String schemaFilePath) {
        this.csvFilePath = csvFilePath;
        this.schema = new Schema(schemaFilePath);
        this.events = new ArrayList<>();
        loadEvents();
    }

    private void loadEvents() {
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            int eventCount = 0; // Start counting from 1
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length != schema.getKeys().size() -1 ) { // Subtract 1 for the added CNT field
                    throw new IllegalArgumentException("CSV row doesn't match schema size. Row: " + line + ", Expected size: " + (schema.getKeys().size() - 1) + ", Actual size: " + values.length);
                }

                ArrayList<String> payload = new ArrayList<>();
                payload.add(String.valueOf(eventCount)); // Add the count as the first element
                for (String value : values) {
                    payload.add(value);
                }
                events.add(new CSVPointEvent(payload));
                eventCount++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<PointEvent> iterator() {
        return events.iterator();
    }

    public static void main(String[] args) {
        // Create the CSV event converter
        String csvFilePath = "src/main/resources/data/1m_linear_accel.csv"; // Replace with your CSV file path
        String schemaFilePath = "src/main/resources/domain/linear_accel.conf"; // Replace with your schema file path
        CSVEventConverter converter = new CSVEventConverter(csvFilePath, schemaFilePath);

        // Iterate through the events and print the first 20
        int count = 0;
        for (PointEvent event : converter) {
            if (count >= 20) break;
            System.out.println(event.getPayload());
            count++;
        }
    }
}
