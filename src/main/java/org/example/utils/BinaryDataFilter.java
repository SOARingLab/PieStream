package org.example.utils;

import org.example.events.Attribute;
import org.example.parser.Schema;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.*;

public class BinaryDataFilter {
    private Schema schema;
    private List<Attribute> attributes;

    public BinaryDataFilter(Schema schema) {
        this.schema = schema;
        this.attributes = schema.getAttributes();
    }

    public void filter(String inputFilePath, String outputFilePath, int targetVID) throws IOException {
        // 获取输入文件的总大小（字节数）
        long totalBytes = Files.size(Paths.get(inputFilePath));

        // 用于跟踪已处理的字节数
        long processedBytes = 0;

        // 定义格式化输出
        DecimalFormat decimalFormat = new DecimalFormat("#0.00");

        // 初始化数据流
        try (DataInputStream dataInputStream = new DataInputStream(
                new BufferedInputStream(new FileInputStream(inputFilePath)));
             DataOutputStream dataOutputStream = new DataOutputStream(
                     new BufferedOutputStream(new FileOutputStream(outputFilePath)))) {

            // 用于定期输出进度
            long lastPrintedBytes = 0;
            long printIntervalBytes = totalBytes / 100; // 每处理1%的数据输出一次

            while (true) {
                try {
                    Map<String, Object> recordMap = new LinkedHashMap<>();
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

                    // 如果VID等于目标值，写入输出文件
                    if (vid == targetVID) {
                        // 将记录写入输出文件
                        for (Attribute attribute : attributes) {
                            String type = attribute.getType().toLowerCase();
                            Object value = recordMap.get(attribute.getName());
                            switch (type) {
                                case "byte":
                                    dataOutputStream.writeByte((Byte) value);
                                    break;
                                case "short":
                                    dataOutputStream.writeShort((Short) value);
                                    break;
                                case "int":
                                case "integer":
                                    dataOutputStream.writeInt((Integer) value);
                                    break;
                                case "long":
                                    dataOutputStream.writeLong((Long) value);
                                    break;
                                case "float":
                                    dataOutputStream.writeFloat((Float) value);
                                    break;
                                case "double":
                                    dataOutputStream.writeDouble((Double) value);
                                    break;
                                case "string":
                                    dataOutputStream.writeUTF((String) value);
                                    break;
                                default:
                                    throw new IllegalArgumentException("未知的数据类型: " + attribute.getType());
                            }
                        }
                    }

                    // 定期输出进度（例如每处理1%的数据）
                    if (processedBytes - lastPrintedBytes >= printIntervalBytes || processedBytes == totalBytes) {
                        double progress = (double) processedBytes / totalBytes * 100;
                        System.out.println("处理进度：" + decimalFormat.format(progress) + "%");
                        lastPrintedBytes = processedBytes;
                    }
                } catch (EOFException e) {
                    // 已到达文件末尾，结束读取
                    break;
                }
            }

            System.out.println("数据过滤完成，结果已保存到：" + outputFilePath);
        } catch (IOException e) {
            throw e;
        }
    }

    // 主函数示例
    public static void main(String[] args) {
        String inputFilePath = "/Users/czq/Code/TPstream/data/linear_accel.events";
        String outputFilePath = "/Users/czq/Code/TPstream/data/linear_accel_filtered_404.events";
        String schemaFilePath = "src/main/resources/domain/linear_accel.yaml";

        // 加载Schema
        Schema schema = new Schema(schemaFilePath);

        // 创建过滤器实例
        BinaryDataFilter dataFilter = new BinaryDataFilter(schema);

        // 目标VID
        int targetVID = 404;

        try {
            dataFilter.filter(inputFilePath, outputFilePath, targetVID);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
