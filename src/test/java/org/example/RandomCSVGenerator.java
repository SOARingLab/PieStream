package org.example;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class RandomCSVGenerator {

    public static void generateCSV(int rows, int columns, String filePath) {
        Random random = new Random();

        try (FileWriter writer = new FileWriter(filePath)) {
            for (int i = 0; i < rows; i++) {
                StringBuilder row = new StringBuilder();
                for (int j = 0; j < columns; j++) {
                    row.append(random.nextInt(2)); // 随机生成0或1
                    if (j < columns - 1) {
                        row.append(","); // 列之间用逗号分隔
                    }
                }
                writer.write(row.toString() + "\n"); // 每行结束换行
            }
            System.out.println("CSV文件已生成: " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        int n = 7; // 列数
        int m = 50000; // 行数
        String filePath = "src/test/resources/data/col"+String.valueOf(n)+"_row"+String.valueOf(m)+".csv"; // 文件保存路径

        generateCSV(m, n, filePath);
    }
}
