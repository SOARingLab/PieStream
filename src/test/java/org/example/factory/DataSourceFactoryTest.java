package org.example.factory;


import org.example.datasource.DataSource;
import org.example.datasource.FileDataSource;
import org.example.datasource.StreamDataSource;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataSourceFactoryTest {

    private Path tempFile;
    private ByteArrayInputStream inputStream;

    @BeforeEach
    public void setUp() throws IOException {
        // Create a temporary file for testing
        tempFile = Files.createTempFile("testFile", ".csv");
        Files.write(tempFile, "line1\nline2\nline3".getBytes());

        // Create a ByteArrayInputStream for testing
        inputStream = new ByteArrayInputStream("line1\nline2\nline3".getBytes());
    }

    @AfterEach
    public void tearDown() throws IOException {
        // Clean up the temporary file
        Files.deleteIfExists(tempFile);
        if (inputStream != null) {
            inputStream.close();
        }
    }

    @Test
    public void testCreateFileDataSource() throws IOException {
        DataSource fileDataSource = DataSourceFactory.createFileDataSource(tempFile.toString());
        assertNotNull(fileDataSource, "FileDataSource should not be null");
        fileDataSource.close();
    }

    @Test
    public void testCreateStreamDataSource() {
        DataSource streamDataSource = DataSourceFactory.createStreamDataSource(inputStream);
        assertNotNull(streamDataSource, "StreamDataSource should not be null");
        streamDataSource.close();
    }

    @Test
    public void testFileDataSourceRead() throws IOException {
        DataSource fileDataSource = DataSourceFactory.createFileDataSource(tempFile.toString());
        String line = fileDataSource.readNext();
        assertTrue(line.contains("line1"), "FileDataSource should read the first line correctly");
        fileDataSource.close();
    }

    @Test
    public void testStreamDataSourceRead() {
        DataSource streamDataSource = DataSourceFactory.createStreamDataSource(inputStream);
        String line = streamDataSource.readNext();
        assertTrue(line.contains("line1"), "StreamDataSource should read the first line correctly");
        streamDataSource.close();
    }
}