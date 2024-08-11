package org.example.datasource;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;

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
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing stream", e);
        }
    }
}