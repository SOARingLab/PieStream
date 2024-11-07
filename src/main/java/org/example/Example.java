//package org.example;
//
//import org.example.datasource.DataSource;
//import org.example.datasource.FileDataSource;
//import org.example.engine.Engine;
//import org.example.events.Attribute;
//import org.example.parser.Schema;
//
//import java.io.IOException;
//
//public class Example {
//    public static void main(String[] args) {
//        // Kafka Stream 配置参数
//        String bootstrapServers = "localhost:9092"; // Kafka broker 地址
//        String applicationId = "kafka-stream-prodsce2d2ss2das"; // Kafka Stream 应用 ID
//        String inputTopic = "linear-tt"; // 输入的 Kafka 主题
//
//        // 配置文件路径
//        String schemaFilePath = "src/main/resources/domain/linear_accel.yaml";
//        Schema schema = new Schema(schemaFilePath); // 加载 Schema
//
//////         查询语句
//        String query = "SELECT s.ts, s.te " +
//                "FROM CarStream " +
//                "DEFINE X AS XWay > 2, S AS SPEED > 30,D AS ACCEL <= -0.5 " +
//                "PATTERN S  follow;followed-by;meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals   X  " +
//                "AND S follow;followed-by;meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals    D "+
//                "AND D follow;followed-by;meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals   X "+
//                "WINDOW 10000";
//
//        //         查询语句
////        String query = "SELECT s.ts, s.te " +
////                "FROM CarStream " +
////                "DEFINE   S AS SPEED > 30,D AS ACCEL <= -0.5 " +
////                "PATTERN   " +
////                "  S follow;followed-by;meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals  D "+
////                "WINDOW 5 min";
//        // 定义属性
//        Attribute partAtb = new Attribute("VID", "int");
//
//
//        // 创建 Engine 实例
//        Engine engine = new Engine(schema, query);
//
//        // 文件数据源，读取数据并应用到 Engine 中
//        try (DataSource dataSource = new FileDataSource("src/main/resources/data/fake.csv")) {
//            String line;
//            while ((line = dataSource.readNext()) != null) {
//                System.out.println("Line Read: " + line);
//                engine.apply("", line); // 处理每一行数据
//            }
//        } catch (IOException e) {
//
//            System.err.println("Failed to open file: " + e.getMessage());
//        }
//    }
//}
