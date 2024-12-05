package org.example.datasource;

import org.example.engine.Engine;
import org.example.events.Attribute;
import org.example.parser.Schema;
import com.google.common.util.concurrent.RateLimiter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileDataSource implements DataSource {
    private BufferedReader reader;
    private long limit;
    private long readCount;
    private RateLimiter rateLimiter; // 用于速率控制


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

    public FileDataSource(String filePath, long limit, long rates) throws IOException {
        reader = new BufferedReader(new FileReader(filePath));
        this.limit = limit;
        this.readCount = 0; // 初始化已读取的数据量
        if (rates > 0) {
            this.rateLimiter = RateLimiter.create(rates); // 初始化 RateLimiter
        } else {
            this.rateLimiter = null; // 如果 rates <= 0，则不进行速率限制
        }
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
            if (rateLimiter != null) {
                rateLimiter.acquire(); // 进行速率限制
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
                if (rateLimiter != null) {
                    rateLimiter.acquire(); // 进行速率限制
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

    /**
     * 简单的 main 方法用于测试 FileDataSource 类。
     * 该方法将展示如何使用不同的构造函数来读取 CSV 文件。
     */
    public static void main(String[] args) {
        // 确保你有一个大的 CSV 文件，例如 "large_file.csv" 在指定路径
        String filePath = "/Users/czq/Code/TPS_data/events_col4_row10000000.csv";

        // 测试 1: 使用原始构造函数，读取所有数据
        System.out.println("=== 测试 1: 读取所有数据 ===");
        try (DataSource dataSource = new FileDataSource(filePath)) {
            String line;
            long startTime = System.currentTimeMillis();
            long count = 0;
            while ((line = dataSource.readNext()) != null) {
                // 这里可以选择不打印每一行，以提高性能
                // System.out.println(line);
                count++;
                if (count % 100000 == 0) {
                    System.out.println("已读取 " + count + " 行");
                }
            }
            long endTime = System.currentTimeMillis();
            double durationSeconds = (endTime - startTime) / 1000.0;
            System.out.println("总共读取 " + count + " 行，耗时 " + durationSeconds + " 秒");
        } catch (IOException e) {
            System.err.println("无法打开文件: " + e.getMessage());
        }

        // 测试 2: 使用带 limit 和 rate 的构造函数，读取前 1000 行，速率为 50 行/秒
        System.out.println("\n=== 测试 2: 读取前 1000 行，速率限制为 50 行/秒 ===");
        long limit = 1000;
        long rate = 50; // 50 行每秒
        try (DataSource dataSource = new FileDataSource(filePath, limit, rate)) {
            String line;
            long startTime = System.currentTimeMillis();
            long count = 0;
            while ((line = dataSource.readNext()) != null) {
                // 这里可以选择不打印每一行，以提高性能
                // System.out.println(line);
                count++;
                if (count % 100 == 0) {
                    System.out.println("已读取 " + count + " 行");
                }
            }
            long endTime = System.currentTimeMillis();
            double durationSeconds = (endTime - startTime) / 1000.0;
            System.out.println("总共读取 " + count + " 行，耗时 " + durationSeconds + " 秒");
            System.out.println("预期耗时约 " + (limit / (double) rate) + " 秒");
        } catch (IOException e) {
            System.err.println("无法打开文件: " + e.getMessage());
        }

        // 测试 3: 使用带 limit 和 rate 的构造函数，读取前 2000 行，速率为 100 行/秒
        System.out.println("\n=== 测试 3: 读取前 2000 行，速率限制为 100 行/秒 ===");
        limit = 2000;
        rate = 100; // 100 行每秒
        try (DataSource dataSource = new FileDataSource(filePath, limit, rate)) {
            String line;
            long startTime = System.currentTimeMillis();
            long count = 0;
            while ((line = dataSource.readNext()) != null) {
                // 这里可以选择不打印每一行，以提高性能
                // System.out.println(line);
                count++;
                if (count % 500 == 0) {
                    System.out.println("已读取 " + count + " 行");
                }
            }
            long endTime = System.currentTimeMillis();
            double durationSeconds = (endTime - startTime) / 1000.0;
            System.out.println("总共读取 " + count + " 行，耗时 " + durationSeconds + " 秒");
            System.out.println("预期耗时约 " + (limit / (double) rate) + " 秒");
        } catch (IOException e) {
            System.err.println("无法打开文件: " + e.getMessage());
        }
    }
}
