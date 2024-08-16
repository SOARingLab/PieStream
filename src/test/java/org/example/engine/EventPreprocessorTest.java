//package org.example.engine;
//
//import org.example.events.PointEvent;
//import org.example.events.Schema;
//import org.junit.jupiter.api.*;
//
//import java.io.FileWriter;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//public class EventPreprocessorTest {
//
//    private static final String SCHEMA_FILE_WITH_TIMESTAMP = "src/test/resources/test-schema-with-timestamp.yaml";
//    private static final String SCHEMA_FILE_WITHOUT_TIMESTAMP = "src/test/resources/test-schema-without-timestamp.yaml";
//    private Schema schemaWithTimestamp;
//    private Schema schemaWithoutTimestamp;
//    private EventPreprocessor eventPreprocessor;
//
//    @BeforeAll
//    public static void generateSchemaFiles() throws IOException {
//        // 动态生成带时间戳的 YAML 文件
//        try (FileWriter fileWriter = new FileWriter(SCHEMA_FILE_WITH_TIMESTAMP)) {
//            fileWriter.write("timestamp: Pos\n" +
//                    "fields:\n" +
//                    "  - name: VID\n" +
//                    "    type: int\n" +
//                    "  - name: SPEED\n" +
//                    "    type: int\n" +
//                    "  - name: ACCEL\n" +
//                    "    type: float\n" +
//                    "  - name: XWay\n" +
//                    "    type: int\n" +
//                    "  - name: Lane\n" +
//                    "    type: int\n" +
//                    "  - name: Dir\n" +
//                    "    type: int\n" +
//                    "  - name: Seg\n" +
//                    "    type: int\n" +
//                    "  - name: Pos\n" +
//                    "    type: long\n");
//        }
//
//        // 动态生成不带时间戳的 YAML 文件
//        try (FileWriter fileWriter = new FileWriter(SCHEMA_FILE_WITHOUT_TIMESTAMP)) {
//            fileWriter.write("fields:\n" +
//                    "  - name: VID\n" +
//                    "    type: int\n" +
//                    "  - name: SPEED\n" +
//                    "    type: int\n" +
//                    "  - name: ACCEL\n" +
//                    "    type: float\n" +
//                    "  - name: XWay\n" +
//                    "    type: int\n" +
//                    "  - name: Lane\n" +
//                    "    type: int\n" +
//                    "  - name: Dir\n" +
//                    "    type: int\n" +
//                    "  - name: Seg\n" +
//                    "    type: int\n");
//        }
//    }
//
//    @AfterAll
//    public static void deleteSchemaFiles() throws IOException {
//        // 删除测试用的 YAML 文件
//        Files.deleteIfExists(Paths.get(SCHEMA_FILE_WITH_TIMESTAMP));
//        Files.deleteIfExists(Paths.get(SCHEMA_FILE_WITHOUT_TIMESTAMP));
//    }
//
//    @BeforeEach
//    public void setUp() {
//        // 加载动态生成的 Schema 文件
//        schemaWithTimestamp = new Schema(SCHEMA_FILE_WITH_TIMESTAMP);
//        schemaWithoutTimestamp = new Schema(SCHEMA_FILE_WITHOUT_TIMESTAMP);
//    }
//
//    @Test
//    public void testPreprocessWithNativeTimestamp() {
//        eventPreprocessor = new EventPreprocessor(schemaWithTimestamp);
//
//        // 构建带有时间戳的事件
//        Map<String, Object> rawEvent = new HashMap<>();
//        rawEvent.put("VID", 123);
//        rawEvent.put("SPEED", 60);
//        rawEvent.put("Pos", 100L);  // 时间戳字段
//
//        PointEvent pointEvent = eventPreprocessor.preprocess(rawEvent);
//
//        assertEquals(100L, pointEvent.getTimestamp());
//        assertEquals(123, pointEvent.getPayload().get("VID"));
//        assertEquals(60, pointEvent.getPayload().get("SPEED"));
//        assertEquals(100L, pointEvent.getPayload().get("Pos"));
//    }
//
//    @Test
//    public void testPreprocessWithoutNativeTimestamp() {
//        eventPreprocessor = new EventPreprocessor(schemaWithoutTimestamp);
//
//        // 构建不带时间戳字段的事件
//        Map<String, Object> rawEvent = new HashMap<>();
//        rawEvent.put("VID", 123);
//        rawEvent.put("SPEED", 60);
//        rawEvent.put("ACCEL", 10.5f);
//        rawEvent.put("XWay", 1);
//        rawEvent.put("Lane", 2);
//        rawEvent.put("Dir", 0);
//        rawEvent.put("Seg", 5);
//        // 注意，这里不包含 "Pos" 字段
//
//        PointEvent pointEvent = eventPreprocessor.preprocess(rawEvent);
//
//        // 验证生成的时间戳
//        assertTrue(pointEvent.getTimestamp() == 0);
//        // 验证其他字段是否正确
//        assertEquals(123, pointEvent.getPayload().get("VID"));
//        assertEquals(60, pointEvent.getPayload().get("SPEED"));
//        assertEquals(10.5f, pointEvent.getPayload().get("ACCEL"));
//        assertEquals(1, pointEvent.getPayload().get("XWay"));
//        assertEquals(2, pointEvent.getPayload().get("Lane"));
//        assertEquals(0, pointEvent.getPayload().get("Dir"));
//        assertEquals(5, pointEvent.getPayload().get("Seg"));
//
//        PointEvent pointEvent2 = eventPreprocessor.preprocess(rawEvent);
//        // 验证生成的时间戳
//        assertTrue(pointEvent2.getTimestamp() == 1);
//        // 验证其他字段是否正确
//        assertEquals(123, pointEvent2.getPayload().get("VID"));
//        assertEquals(60, pointEvent2.getPayload().get("SPEED"));
//        assertEquals(10.5f, pointEvent2.getPayload().get("ACCEL"));
//        assertEquals(1, pointEvent2.getPayload().get("XWay"));
//        assertEquals(2, pointEvent2.getPayload().get("Lane"));
//        assertEquals(0, pointEvent2.getPayload().get("Dir"));
//        assertEquals(5, pointEvent2.getPayload().get("Seg"));
//
//    }
//
//    @Test
//    public void testPreprocessWithListEvent() {
//        eventPreprocessor = new EventPreprocessor(schemaWithTimestamp);
//
//        // 模拟 List 类型的事件
//        List<Object> rawEventList = Arrays.asList(123, 60, 10.5f, 1, 2, 0, 5, 100L);
//
//        PointEvent pointEvent = eventPreprocessor.preprocess(rawEventList);
//
//        assertEquals(100L, pointEvent.getTimestamp());
//        assertEquals(123, pointEvent.getPayload().get("VID"));
//        assertEquals(60, pointEvent.getPayload().get("SPEED"));
//    }
//
//    @Test
//    public void testPreprocessWithMapEvent() {
//        eventPreprocessor = new EventPreprocessor(schemaWithTimestamp);
//
//        // 模拟 Map 类型的事件
//        Map<String, Object> rawEventMap = new HashMap<>();
//        rawEventMap.put("VID", 123);
//        rawEventMap.put("SPEED", 60);
//        rawEventMap.put("Pos", 100L);
//
//        PointEvent pointEvent = eventPreprocessor.preprocess(rawEventMap);
//
//        assertEquals(100L, pointEvent.getTimestamp());
//        assertEquals(123, pointEvent.getPayload().get("VID"));
//        assertEquals(60, pointEvent.getPayload().get("SPEED"));
//    }
//}
