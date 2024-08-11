package org.example.datasource;


public interface DataSource {
    String readNext();
    void close();
}
