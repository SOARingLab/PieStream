package org.example.datasource;

import java.util.List;

public interface DataSource extends AutoCloseable {
    String readNext();
    boolean hasNext();
    List<String> readBatch(int batchSize);
    void close();
}
