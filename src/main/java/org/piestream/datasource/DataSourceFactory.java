package org.piestream.datasource;

import java.io.InputStream;

/**
 * Factory class for creating instances of different types of DataSource.
 * This class provides static methods for creating data sources from various sources,
 * such as files and input streams. It abstracts the instantiation logic and handles
 * any exceptions that may occur during the creation process.
 */
public class DataSourceFactory {

    /**
     * Creates a DataSource instance for reading data from a file.
     *
     * @param filePath the path to the file from which data will be read
     * @return a DataSource instance for the given file
     * @throws RuntimeException if there is an error creating the file data source
     */
    public static DataSource createFileDataSource(String filePath) {
        try {
            return new FileDataSource(filePath);
        } catch (Exception e) {
            throw new RuntimeException("Error creating file data source", e);
        }
    }

    /**
     * Creates a DataSource instance for reading data from an InputStream.
     *
     * @param inputStream the input stream from which data will be read
     * @return a DataSource instance for the given input stream
     */
    public static DataSource createStreamDataSource(InputStream inputStream) {
        return new StreamDataSource(inputStream);
    }
}
