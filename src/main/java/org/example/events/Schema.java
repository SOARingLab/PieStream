package org.example.events;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Schema {
    private final Map<Attribute, Integer> schema;

    public Schema(String schemaFilePath) {
        this.schema = loadSchema(schemaFilePath);
    }

    private Map<Attribute, Integer> loadSchema(String schemaFilePath) {
        Map<Attribute, Integer> schema = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(schemaFilePath))) {
            String line;
            schema.put(new Attribute("CNT"), 0); // CNT is always mapped to index
            if ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                for (int index = 0; index < parts.length; index++) {
                    schema.put(new Attribute(parts[index].trim()), index+1);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return schema;
    }

    public Set<Attribute> getKeys() {
        return schema.keySet();
    }

    public int getIndex(Attribute key) {
        Integer index = schema.get(key);
        if (index != null) {
            return index;
        }
        throw new IllegalArgumentException("Attribute not found in schema: " + key.getName());
    }

    public String getValue(Attribute key, PointEvent event) {
        int index = getIndex(key);
        return event.getPayload().get(index);
    }
}
