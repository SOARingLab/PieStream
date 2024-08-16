//package org.example.events;
//
//import org.example.utils.Config;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.AfterEach;
//
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.List;
//import java.util.Map;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//public class SchemaTest {
//    private static final String SCHEMA_FILE = "src/main/resources/domain/linear_accel.yaml";
//    private Schema schema;
//
//    @BeforeEach
//    public void setUp() throws IOException {
//        // 创建一个示例配置文件用于测试
//        File schemaFile = new File(SCHEMA_FILE);
//        try (FileWriter fileWriter = new FileWriter(schemaFile)) {
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
//        schema = new Schema(SCHEMA_FILE);
//    }
//
//    @AfterEach
//    public void tearDown() throws IOException {
//        // 删除测试用的配置文件
//        Files.deleteIfExists(Paths.get(SCHEMA_FILE));
//    }
//
//    @Test
//    public void testSchemaInitialization() {
//        assertNotNull(schema);
//        assertEquals("Pos", schema.getTimestampField());
//        assertTrue(schema.hasNativeTimestamp());
//    }
//
//    @Test
//    public void testFields() {
//        Map<String, String> fields = schema.getFields();
//        assertEquals(8, fields.size());
//        assertEquals("int", fields.get("VID"));
//        assertEquals("int", fields.get("SPEED"));
//        assertEquals("float", fields.get("ACCEL"));
//        assertEquals("int", fields.get("XWay"));
//        assertEquals("int", fields.get("Lane"));
//        assertEquals("int", fields.get("Dir"));
//        assertEquals("int", fields.get("Seg"));
//        assertEquals("long", fields.get("Pos"));
//    }
//
//    @Test
//    public void testAttributes() {
//        List<Attribute> attributes = schema.getAttributes();
//        assertEquals(8, attributes.size());
//        assertEquals(new Attribute("VID", "int"), attributes.get(0));
//        assertEquals(new Attribute("SPEED", "int"), attributes.get(1));
//        assertEquals(new Attribute("ACCEL", "float"), attributes.get(2));
//        assertEquals(new Attribute("XWay", "int"), attributes.get(3));
//        assertEquals(new Attribute("Lane", "int"), attributes.get(4));
//        assertEquals(new Attribute("Dir", "int"), attributes.get(5));
//        assertEquals(new Attribute("Seg", "int"), attributes.get(6));
//        assertEquals(new Attribute("Pos", "long"), attributes.get(7));
//    }
//
//    @Test
//    public void testFieldIndexMap() {
//        Map<String, Integer> fieldIndexMap = schema.getFieldIndexMap();
//        assertEquals(8, fieldIndexMap.size());
//        assertEquals(0, fieldIndexMap.get("VID"));
//        assertEquals(1, fieldIndexMap.get("SPEED"));
//        assertEquals(2, fieldIndexMap.get("ACCEL"));
//        assertEquals(3, fieldIndexMap.get("XWay"));
//        assertEquals(4, fieldIndexMap.get("Lane"));
//        assertEquals(5, fieldIndexMap.get("Dir"));
//        assertEquals(6, fieldIndexMap.get("Seg"));
//        assertEquals(7, fieldIndexMap.get("Pos"));
//    }
//}
