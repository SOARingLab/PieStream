package org.example.datasource;

import org.example.engine.Engine;
import org.example.events.Attribute;
import org.example.parser.Schema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileDataSource implements DataSource {
    private BufferedReader reader;
    private long limit;
    private long readCount;

    // 原构造函数
    public FileDataSource(String filePath) throws IOException {
        reader = new BufferedReader(new FileReader(filePath));
        this.limit = Long.MAX_VALUE; // 默认不限制读取行数
        this.readCount = 0; // 初始化已读取的数据量
    }

    // 新增的构造函数，接受一个 limit 参数
    public FileDataSource(String filePath, long limit) throws IOException {
        reader = new BufferedReader(new FileReader(filePath));
        this.limit = limit;
        this.readCount = 0; // 初始化已读取的数据量
    }

    @Override
    public boolean hasNext() {
        try {
            if (readCount >= limit) {
                return false; // 如果已经读取超过 limit 个数据，返回 false
            }
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
            if (readCount >= limit) {
                return null; // 如果读取超过 limit，返回 null
            }
            String line = reader.readLine();
            if (line != null) {
                readCount++; // 增加已读取的数据量
            }
            return line;
        } catch (IOException e) {
            throw new RuntimeException("Error reading from file", e);
        }
    }

    @Override
    public List<String> readBatch(int batchSize) {
        List<String> batch = new ArrayList<>();
        try {
            for (int i = 0; i < batchSize; i++) {
                if (readCount >= limit) {
                    break; // 如果已经读取超过 limit，停止读取
                }
                String line = reader.readLine();
                if (line != null) {
                    batch.add(line);
                    readCount++; // 增加已读取的数据量
                } else {
                    break; // 没有更多数据了
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
                "WINDOW 1000000";


        // 创建 Engine 实例
        Engine engine = new Engine(schema, query);

        // 使用无参数构造函数，读取所有数据
        try (DataSource dataSource = new FileDataSource("src/main/resources/data/fake.csv")) {
            String line;
            while ((line = dataSource.readNext()) != null) {
                System.out.println("Line Read: " + line);
                engine.apply("", line); // 处理每一行数据
            }
        } catch (IOException e) {
            System.err.println("Failed to open file: " + e.getMessage());
        }

        // 使用带 limit 参数的构造函数，读取前 100 行数据
        try (DataSource dataSource = new FileDataSource("src/main/resources/data/fake.csv", 100)) {
            String line;
            while ((line = dataSource.readNext()) != null) {
                System.out.println("Line Read (with limit): " + line);
                engine.apply("", line); // 处理每一行数据
            }
        } catch (IOException e) {
            System.err.println("Failed to open file: " + e.getMessage());
        }
    }
}
