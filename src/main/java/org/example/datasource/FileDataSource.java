package org.example.datasource;

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
}
