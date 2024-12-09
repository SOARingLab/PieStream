package org.piestream.datasource;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.piestream.engine.Engine;
import org.piestream.events.Attribute;
import org.piestream.parser.Schema;

import java.io.*;
import java.util.*;

public class BinaryDataSource implements DataSource {
    private DataInputStream dataInputStream;
    private Schema schema;
    private List<Attribute> attributes;
    private boolean hasNextRecord;
    private String nextRecord;
    private ObjectMapper objectMapper; // 声明 ObjectMapper

    public BinaryDataSource(String binaryFilePath, Schema schema) throws IOException {
        // 加载Schema
        this.schema = schema;
        this.attributes = schema.getAttributes();

        // 初始化DataInputStream
        FileInputStream fileInputStream = new FileInputStream(binaryFilePath);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        this.dataInputStream = new DataInputStream(bufferedInputStream);

        // 初始化 ObjectMapper
        this.objectMapper = new ObjectMapper();

        // 预读取第一条记录
        this.nextRecord = readNextRecord();
        this.hasNextRecord = this.nextRecord != null;
    }

    private String readNextRecord() {
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
                        throw new IllegalArgumentException("未知的数据类型: " + attribute.getType());
                }
            }
            // 使用 ObjectMapper 将记录转换为 JSON 字符串
            String jsonString = objectMapper.writeValueAsString(recordMap);
            return jsonString;
        } catch (EOFException e) {
            // 已到达文件末尾
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String readNext() {
        String currentRecord = this.nextRecord;
        this.nextRecord = readNextRecord();
        this.hasNextRecord = this.nextRecord != null;
        return currentRecord;
    }

    @Override
    public boolean hasNext() {
        return hasNextRecord;
    }

    @Override
    public List<String> readBatch(int batchSize) {
        List<String> batch = new ArrayList<>();
        for (int i = 0; i < batchSize && hasNext(); i++) {
            batch.add(readNext());
        }
        return batch;
    }

    @Override
    public void close() {
        try {
            dataInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String binaryFilePath = "/Users/czq/Code/TPstream/data/linear_accel_filtered_404.events";
        String schemaFilePath = "src/main/resources/domain/linear_accel.yaml";
        Schema schema = new Schema(schemaFilePath); // 加载 Schema

        String query = "SELECT s.ts, s.te " +
                "FROM CarStream " +
                "DEFINE D AS ACCEL <= -0.00455 , S AS SPEED >= 32 , A AS ACCEL >= 0.0050 " +
                "PATTERN " +
                "A  meets; overlaps; starts; during ; before    S  " +
                "AND S  meets; contains; followed-by; overlaps;after;before   D  " +
                "WINDOW 1000000";

         // 设置队列容量

        // 创建 Engine 实例
        Engine engine = new Engine(schema, query);

        try (DataSource dataSource = new BinaryDataSource(binaryFilePath, schema)) {
            long time = -System.nanoTime();
            while (dataSource.hasNext()) {
                String record = dataSource.readNext();
                // 输出 JSON 格式的记录
//                System.out.println(record);
                engine.apply("", record); // 处理每一行数据
            }

            time += System.nanoTime();

            engine.formatResult();
            engine.printResultCNT();
            System.out.println( (time / 1_000_000) + "ms");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
