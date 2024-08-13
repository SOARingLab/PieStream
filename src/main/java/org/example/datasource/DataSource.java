package org.example.datasource;

import java.util.List;

public interface DataSource {
    String readNext();
    List<String> readBatch(int batchSize);
    void close();
}
