package org.example.datasource;

import org.example.engine.Engine;
import org.example.events.Attribute;
import org.example.events.Schema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileDataSource implements DataSource {
    private BufferedReader reader;

    public FileDataSource(String filePath) throws IOException {
        reader = new BufferedReader(new FileReader(filePath));
    }

    @Override
    public boolean hasNext() {
        try {
            reader.mark(1); // 标记当前位置
            if (reader.readLine() == null) {
                return false; // 文件没有更多内容
            }
            reader.reset(); // 重置到标记的位置
            return true; // 文件中还有更多内容
        } catch (IOException e) {
            throw new RuntimeException("Error reading from file", e);
        }
    }


    @Override
    public String readNext() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            throw new RuntimeException("Error reading from file", e);
        }
    }

    @Override
    public List<String> readBatch(int batchSize) {
        List<String> batch = new ArrayList<>();
        try {
            for (int i = 0; i < batchSize; i++) {
                String line = reader.readLine();
                if (line != null) {
                    batch.add(line);
                } else {
                    break; // No more data available
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading from file", e);
        }
        return batch;
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing file", e);
        }
    }
    public static void main(String[] args) {


        // 配置文件路径
        String schemaFilePath = "src/main/resources/domain/linear_accel.yaml";
        Schema schema = new Schema(schemaFilePath); // 加载 Schema

        String query = "SELECT s.ts, s.te " +
                "FROM CarStream " +
                "DEFINE X AS XWay > 2, S AS SPEED > 30,D AS ACCEL <= -0.5 " +
                "PATTERN S follow;followed-by;meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals  X  " +
                "AND S follow;followed-by;meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals  D "+
                "AND D follow;followed-by;meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals  X "+
                "WINDOW 5 min";

        // 定义属性
        Attribute partAtb = new Attribute("VID", "int");

        int QCapacity = 10000; // 设置队列容量

        // 创建 Engine 实例
        Engine engine = new Engine(schema, partAtb, QCapacity, query);

        // 文件数据源，读取数据并应用到 Engine 中
        try (DataSource dataSource = new FileDataSource("src/main/resources/data/fake.csv")) {
            String line;
            while ((line = dataSource.readNext()) != null) {
                System.out.println("Line Read: " + line);
                engine.apply("", line); // 处理每一行数据
            }
        } catch (IOException e) {
            System.err.println("Failed to open file: " + e.getMessage());
        }
    }
}
