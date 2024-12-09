package org.piestream.utils;

import org.piestream.events.Attribute;
import org.piestream.parser.Schema;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class BinaryDataCounter {
    private Schema schema;
    private List<Attribute> attributes;

    public BinaryDataCounter(Schema schema) {
        this.schema = schema;
        this.attributes = schema.getAttributes();
    }

    public void countVIDRecords(String inputFilePath) throws IOException {
        // 用于存储每个VID的记录数
        Map<Integer, Integer> vidCounts = new HashMap<>();

        // 获取输入文件的总大小（字节数）
        long totalBytes = Files.size(Paths.get(inputFilePath));

        // 用于跟踪已处理的字节数
        long processedBytes = 0;

        // 初始化数据流
        try (DataInputStream dataInputStream = new DataInputStream(
                new BufferedInputStream(new FileInputStream(inputFilePath)))) {

            while (true) {
                try {
                    Map<String, Object> recordMap = new HashMap<>();
                    // 读取一条记录
                    for (Attribute attribute : attributes) {
                        String type = attribute.getType().toLowerCase();
                        switch (type) {
                            case "byte":
                                byte byteValue = dataInputStream.readByte();
                                recordMap.put(attribute.getName(), byteValue);
                                processedBytes += 1; // byte占1个字节
                                break;
                            case "short":
                                short shortValue = dataInputStream.readShort();
                                recordMap.put(attribute.getName(), shortValue);
                                processedBytes += 2; // short占2个字节
                                break;
                            case "int":
                            case "integer":
                                int intValue = dataInputStream.readInt();
                                recordMap.put(attribute.getName(), intValue);
                                processedBytes += 4; // int占4个字节
                                break;
                            case "long":
                                long longValue = dataInputStream.readLong();
                                recordMap.put(attribute.getName(), longValue);
                                processedBytes += 8; // long占8个字节
                                break;
                            case "float":
                                float floatValue = dataInputStream.readFloat();
                                recordMap.put(attribute.getName(), floatValue);
                                processedBytes += 4; // float占4个字节
                                break;
                            case "double":
                                double doubleValue = dataInputStream.readDouble();
                                recordMap.put(attribute.getName(), doubleValue);
                                processedBytes += 8; // double占8个字节
                                break;
                            case "string":
                                String stringValue = dataInputStream.readUTF();
                                recordMap.put(attribute.getName(), stringValue);
                                // UTF字符串的长度无法预知，需要计算实际占用的字节数
                                processedBytes += 2 + stringValue.getBytes("UTF-8").length;
                                break;
                            default:
                                throw new IllegalArgumentException("未知的数据类型: " + attribute.getType());
                        }
                    }

                    // 获取VID的值
                    int vid = (int) recordMap.get("VID");

                    // 更新VID的计数
                    vidCounts.put(vid, vidCounts.getOrDefault(vid, 0) + 1);

                } catch (EOFException e) {
                    // 已到达文件末尾，结束读取
                    break;
                }
            }

            // 创建一个列表，用于排序
            List<Map.Entry<Integer, Integer>> sortedVidCounts = new ArrayList<>(vidCounts.entrySet());

            // 按照记录数从高到低排序
            sortedVidCounts.sort((e1, e2) -> e2.getValue().compareTo(e1.getValue()));

            // 输出记录数最高的前20个VID
            System.out.println("记录数最高的前20个VID：");
            int topN = 20;
            for (int i = 0; i < Math.min(topN, sortedVidCounts.size()); i++) {
                Map.Entry<Integer, Integer> entry = sortedVidCounts.get(i);
                System.out.println("第" + (i + 1) + "名 - VID: " + entry.getKey() + ", 记录数: " + entry.getValue());
            }

        } catch (IOException e) {
            throw e;
        }
    }

    // 主函数示例
    public static void main(String[] args) {
        String inputFilePath = "/Users/czq/Code/TPstream/data/linear_accel.events";
        String schemaFilePath = "src/main/resources/domain/linear_accel.yaml";

        // 加载Schema
        Schema schema = new Schema(schemaFilePath);

        // 创建计数器实例
        BinaryDataCounter dataCounter = new BinaryDataCounter(schema);

        try {
            dataCounter.countVIDRecords(inputFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
