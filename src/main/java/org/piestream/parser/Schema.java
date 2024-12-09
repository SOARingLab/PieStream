package org.piestream.parser;

import org.piestream.events.Attribute;
import org.piestream.utils.Config;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class Schema {
    private String rawdataType;
    private String timestampField;
    private Map<String, String> fields;
    private boolean hasNativeTimestamp;
    private List<Attribute> attributes;
    private Map<String, Integer> fieldIndexMap;
    private TimestampUnit timestampUnit=TimestampUnit.S; // default as SECOND


    public enum TimestampUnit{
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
        // 获取该单位对应的纳秒数
        public long getNanosPerUnit() {
            switch (this) {
                case S:     return 1000000000L;
                case MS:    return 1000000L;
                case US:    return 1000L;
                case NS:    return 1L;
                default: throw new IllegalStateException("Unexpected value: " + this);
            }
        }
        // 将String转换为对应的TimestampUnit枚举
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

    public Schema(String schemaFilePath) {
        this.fields = new HashMap<>();
        this.attributes = new ArrayList<>();
        this.fieldIndexMap = new HashMap<>();
        loadSchema(schemaFilePath);
    }
    public Schema(String rawdataType, String timestampField,String  timestampUnit, List<Attribute> attributes) {
        this.rawdataType = rawdataType;
        this.timestampField = timestampField;
        this.hasNativeTimestamp = timestampField != null && !timestampField.isEmpty();
        this.fields = new HashMap<>();
        this.attributes = attributes != null ? new ArrayList<>(attributes) : new ArrayList<>();
        this.fieldIndexMap = new HashMap<>();
        this.timestampUnit = TimestampUnit.fromString(timestampUnit);

        // 初始化 fields 和 fieldIndexMap
        if (attributes != null) {
            for (int i = 0; i < attributes.size(); i++) {
                Attribute attribute = attributes.get(i);
                fields.put(attribute.getName(), attribute.getType());
                fieldIndexMap.put(attribute.getName(), i);
            }
        }
    }
    public Schema(String rawdataType, String timestampField, List<Attribute> attributes) {
        this.rawdataType = rawdataType;
        this.timestampField = timestampField;
        this.hasNativeTimestamp = timestampField != null && !timestampField.isEmpty();
        this.fields = new HashMap<>();
        this.attributes = attributes != null ? new ArrayList<>(attributes) : new ArrayList<>();
        this.fieldIndexMap = new HashMap<>();

        // 初始化 fields 和 fieldIndexMap
        if (attributes != null) {
            for (int i = 0; i < attributes.size(); i++) {
                Attribute attribute = attributes.get(i);
                fields.put(attribute.getName(), attribute.getType());
                fieldIndexMap.put(attribute.getName(), i);
            }
        }
    }
    public Schema(String rawdataType,   List<Attribute> attributes) {
        this(rawdataType,null,attributes);
    }

    private void loadSchema(String schemaFilePath) {
        try {
            Config config = Config.loadConfig(schemaFilePath);
            this.rawdataType = config.getRawdataType(); // 获取 rawdataType
            this.timestampField = config.getTimestamp();
            this.hasNativeTimestamp = timestampField != null && !timestampField.isEmpty();
            this.fields.clear();
            this.attributes.clear();
            this.fieldIndexMap.clear();
            String unit= config.getTimestampUnit();
            if(unit!=null){
                this.timestampUnit=TimestampUnit.fromString(unit);
            }else{
                this.timestampUnit=TimestampUnit.S;
            }

            for (Config.Field field : config.getFields()) {
                fields.put(field.getName(), field.getType());
                attributes.add(new Attribute(field.getName(), field.getType()));
                fieldIndexMap.put(field.getName(), attributes.size() - 1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TimestampUnit getTimestampUnit() {
        return timestampUnit;
    }

    // Getter for rawdataType
    public String getRawdataType() {
        return rawdataType;
    }

    public String getTimestampField() {
        return timestampField;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public boolean hasNativeTimestamp() {
        return hasNativeTimestamp;
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public Map<String, Integer> getFieldIndexMap() {
        return fieldIndexMap;
    }
}
