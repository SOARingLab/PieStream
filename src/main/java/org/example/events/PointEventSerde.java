package org.example.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;
import org.example.events.PointEvent;

import java.util.Map;

public class PointEventSerde implements Serde<PointEvent> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Serializer<PointEvent> serializer() {
        return new Serializer<PointEvent>() {
            @Override
            public byte[] serialize(String topic, PointEvent data) {
                try {
                    return objectMapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new RuntimeException("Error serializing PointEvent", e);
                }
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public Deserializer<PointEvent> deserializer() {
        return new Deserializer<PointEvent>() {
            @Override
            public PointEvent deserialize(String topic, byte[] data) {
                try {
                    return objectMapper.readValue(data, PointEvent.class);
                } catch (Exception e) {
                    throw new RuntimeException("Error deserializing PointEvent", e);
                }
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}
