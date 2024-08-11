package org.example.factory;

import org.example.datasource.DataSource;
import org.example.datasource.FileDataSource;
import org.example.datasource.StreamDataSource;

import java.io.InputStream;

public class DataSourceFactory {
    public static DataSource createFileDataSource(String filePath) {
        try {
            return new FileDataSource(filePath);
        } catch (Exception e) {
            throw new RuntimeException("Error creating file data source", e);
        }
    }

    public static DataSource createStreamDataSource(InputStream inputStream) {
        return new StreamDataSource(inputStream);
    }
}