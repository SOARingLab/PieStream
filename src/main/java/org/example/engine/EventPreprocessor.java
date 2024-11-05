package org.example.engine;

import org.example.datasource.DataSource;
import org.example.events.Attribute;
import org.example.events.PointEvent;
import org.example.events.PointEventIterator;
import org.example.parser.Schema;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Arrays;

public class EventPreprocessor {
    private final Schema schema;
    private static long timestampCounter = 1; // 用于递增时间戳
    private static final ObjectMapper objectMapper = new ObjectMapper(); // 用于解析 JSON
    private  PointEventIterator pointEventIterator ;


    public EventPreprocessor(Schema schema)  {
        this.schema = schema;
        this.pointEventIterator=new PointEventIterator();
    }

    /**
     * 标准化单个事件，并为缺少时间戳的事件分配时间戳。
     *
     * @param rawPointEvent 原始事件
     * @return 标准化后的 PointEvent
     */
    public PointEvent preprocess(String rawPointEvent) {

        return preprocess(rawPointEvent, false);
    }

    public PointEvent preprocess(String rawPointEvent,boolean useNativeTimestamp ) {
        // 解析原始事件并构建 payload
        Map<Attribute, Object> payload = parseRawEventToPayload(rawPointEvent);

        long timestamp;
        if (useNativeTimestamp && schema.hasNativeTimestamp()){
            timestamp = extractTimestampFromPayload(payload);
        }else{
            // 如果没有原生时间戳，则分配递增时间戳
            synchronized (EventPreprocessor.class) {
                timestamp = timestampCounter++;
            }
        }

        return new PointEvent(payload, timestamp);
    }



    /**
     * 从 payload 中提取时间戳。
     *
     * @param payload 事件的 payload
     * @return 提取出的时间戳
     * @throws IllegalArgumentException 如果找不到时间戳字段
     */
    private long extractTimestampFromPayload(Map<Attribute, Object> payload) {
        // 找到 timestampField 对应的 Attribute
        Attribute timestampAttribute = schema.getAttributes().stream()
                .filter(attr -> attr.getName().equals(schema.getTimestampField()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Timestamp field not found in schema"));

        Object timestampObj = payload.get(timestampAttribute);
        if (timestampObj == null) {
            throw new IllegalArgumentException("Timestamp field not found in event payload");
        }
        if ("long".equalsIgnoreCase(timestampAttribute.getType())) {
            return (Long) timestampObj;
        }


        return Long.parseLong(timestampObj.toString());
    }

    /**
     * 将原始事件转换为 Map 类型的 payload。
     *
     * @param rawEvent 原始事件（可能是 String, List, 或 Map）
     * @return 解析后的 payload
     */
    private Map<Attribute, Object> parseRawEventToPayload(Object rawEvent) {
        Map<Attribute, Object> payload = new HashMap<>();

        String rawdataType = schema.getRawdataType();

        if ("JSON".equalsIgnoreCase(rawdataType)) {
            // 如果是 JSON，则假设 rawEvent 是 JSON 格式的字符串
            if (rawEvent instanceof String) {
                try {
                    // 解析 JSON 字符串为 Map
                    Map<String, Object> rawMap = objectMapper.readValue((String) rawEvent, Map.class);
                    for (Attribute attribute : schema.getAttributes()) {
                        payload.put(attribute, rawMap.get(attribute.getName()));
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to parse JSON string", e);
                }
            } else {
                throw new IllegalArgumentException("For JSON data, rawEvent should be a JSON formatted string");
            }
        }  else if ("CSV".equalsIgnoreCase(rawdataType)) {
            // 如果是 CSV，则 rawEvent 可以是字符串或列表
            if (rawEvent instanceof String) {
                String rawString = (String) rawEvent;
                String[] rawArray = rawString.split(",", -1); // 使用 -1 保留空字符串
                List<String> rawList = Arrays.asList(rawArray);
                return convertListToPayload(rawList);

            } else if (rawEvent instanceof List) {
                return convertListToPayload((List<?>) rawEvent);
            } else {
                throw new IllegalArgumentException("For CSV data, rawEvent should be of type String or List");
            }
        } else if ("BIN".equalsIgnoreCase(rawdataType)) {
            // 如果是 JSON，则假设 rawEvent 是 JSON 格式的字符串
            if (rawEvent instanceof String) {
                try {
                    // 解析 JSON 字符串为 Map
                    Map<String, Object> rawMap = objectMapper.readValue((String) rawEvent, Map.class);
                    for (Attribute attribute : schema.getAttributes()) {
                        payload.put(attribute, rawMap.get(attribute.getName()));
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to parse JSON string", e);
                }
            } else {
                throw new IllegalArgumentException("For JSON data, rawEvent should be a JSON formatted string");
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported rawdataType: " + rawdataType);
        }

        return payload;
    }

    /**
     * 将 List 转换为 Map<Attribute, Object>，使用 Schema 中定义的字段名作为键
     *
     * @param rawList 事件的 List 表示
     * @return 转换后的 Map
     */
    private Map<Attribute, Object> convertListToPayload(List<?> rawList) {
        Map<Attribute, Object> payload = new HashMap<>();
        List<Attribute> attributes = schema.getAttributes();
        if (rawList.size() != attributes.size()) {
            throw new IllegalArgumentException("List size does not match schema field count");
        }
        for (int i = 0; i < rawList.size(); i++) {
            Attribute attribute = attributes.get(i);
            payload.put(attribute, rawList.get(i));
        }
        return payload;
    }

    /**
     * 预处理数据源，返回预处理后的 PointEvent 迭代器。
     *
     * @param dataS 数据源
     * @return 预处理后的 PointEvent 迭代器
     */
    public PointEventIterator preprocessSource(DataSource dataS) {

        while (dataS.hasNext() ) {
            String rawPointEvent = dataS.readNext();
            PointEvent pointEvent = preprocess(rawPointEvent);
            pointEventIterator.addEvent(pointEvent);
        }

        return pointEventIterator;
    }

}
