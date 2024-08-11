package org.example.datasource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class FileDataSource implements DataSource {
    private BufferedReader reader;

    public FileDataSource(String filePath) throws IOException {
        reader = new BufferedReader(new FileReader(filePath));
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
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing file", e);
        }
    }
}