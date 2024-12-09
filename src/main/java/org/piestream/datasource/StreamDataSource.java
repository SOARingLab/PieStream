package org.piestream.datasource;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class StreamDataSource implements DataSource {
    private BufferedReader reader;

    public StreamDataSource(InputStream inputStream) {
        reader = new BufferedReader(new InputStreamReader(inputStream));
    }

    @Override
    public String readNext() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            throw new RuntimeException("Error reading from stream", e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            // 标记当前流的位置，以便可以重置到这个位置
            reader.mark(1);
            if (reader.read() < 0) {
                return false; // 没有更多的数据
            }
            // 重置流的位置回到标记的位置
            reader.reset();
            return true; // 还有数据
        } catch (IOException e) {
            throw new RuntimeException("Error checking next line from stream", e);
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
            throw new RuntimeException("Error reading from stream", e);
        }
        return batch;
    }

    public CompletableFuture<List<String>> readBatchAsync(int batchSize) {
        return CompletableFuture.supplyAsync(() -> readBatch(batchSize));
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing stream", e);
        }
    }
}
