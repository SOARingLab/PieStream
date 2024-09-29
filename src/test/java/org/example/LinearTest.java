package org.example;

import org.example.datasource.BinaryDataSource;
import org.example.datasource.DataSource;
import org.example.datasource.FileDataSource;
import org.example.engine.Engine;
import org.example.events.Attribute;
import org.example.parser.Schema;

import java.io.IOException;

public class LinearTest {
    public static void main(String[] args) {

        // 配置文件路径
        String schemaFilePath = "src/test/resources/domain/linearBin_404.yaml";
        Schema schema = new Schema(schemaFilePath); // 加载 Schema

////         查询语句 521565  521626
        String query = "SELECT s.ts, s.te " +
                "FROM CarStream " +
                "DEFINE   S AS SPEED < 20 ,D AS ACCEL <= 0 " +
                "PATTERN  S follow;followed-by;meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals;before;after    D "+
                "WINDOW 10000";

        // 创建 Engine 实例
        Engine engine = new Engine(schema, query);

        // 文件数据源，读取数据并应用到 Engine 中
        try (DataSource dataSource = new BinaryDataSource("src/test/resources/data/linear_accel_filtered_404.events",schema)) {
            String line;
            while ((line = dataSource.readNext()) != null) {
//                System.out.println("Line Read: " + line);
                engine.apply("", line); // 处理每一行数据
            }
//            engine.formatResult();
            engine.printResultCNT();

        } catch (IOException e) {

            System.err.println("Failed to open file: " + e.getMessage());
        }
    }
}
