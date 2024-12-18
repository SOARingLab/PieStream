package org.piestream.datasource;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * StreamDataSource provides an implementation of the DataSource interface for reading data from an InputStream.
 * This class supports synchronous and asynchronous reading of data in both single-line and batch modes.
 */
public class StreamDataSource implements DataSource {
    private BufferedReader reader; // BufferedReader used to read lines from the InputStream

    /**
     * Constructs a StreamDataSource that reads from the provided InputStream.
     *
     * @param inputStream the InputStream to read data from
     */
    public StreamDataSource(InputStream inputStream) {
        reader = new BufferedReader(new InputStreamReader(inputStream));
    }

    /**
     * Reads the next line from the stream.
     *
     * @return the next line from the stream, or null if end-of-stream is reached
     * @throws RuntimeException if an IOException occurs during reading
     */
    @Override
    public String readNext() {
        try {
            return reader.readLine(); // Read a single line from the stream
        } catch (IOException e) {
            throw new RuntimeException("Error reading from stream", e); // Handle I/O exception
        }
    }

    /**
     * Checks whether there is more data available to read from the stream.
     *
     * @return true if there is more data to read, false otherwise
     * @throws RuntimeException if an IOException occurs during the check
     */
    @Override
    public boolean hasNext() {
        try {
            // Mark the current position in the stream so we can reset back to it
            reader.mark(1);
            if (reader.read() < 0) {
                return false; // End of stream reached
            }
            // Reset the reader position back to the mark
            reader.reset();
            return true; // Data is available to read
        } catch (IOException e) {
            throw new RuntimeException("Error checking next line from stream", e); // Handle I/O exception
        }
    }

    /**
     * Reads a batch of lines from the stream.
     *
     * @param batchSize the number of lines to read in this batch
     * @return a List containing the lines read from the stream
     * @throws RuntimeException if an IOException occurs during reading
     */
    @Override
    public List<String> readBatch(int batchSize) {
        List<String> batch = new ArrayList<>();
        try {
            for (int i = 0; i < batchSize; i++) {
                String line = reader.readLine();
                if (line != null) {
                    batch.add(line); // Add the line to the batch
                } else {
                    break; // No more data available in the stream
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading from stream", e); // Handle I/O exception
        }
        return batch; // Return the batch of lines
    }

    /**
     * Reads a batch of lines from the stream asynchronously.
     *
     * @param batchSize the number of lines to read in this batch
     * @return a CompletableFuture that will supply the batch of lines once read
     */
    public CompletableFuture<List<String>> readBatchAsync(int batchSize) {
        return CompletableFuture.supplyAsync(() -> readBatch(batchSize)); // Asynchronous read
    }

    /**
     * Closes the underlying InputStream and releases resources.
     *
     * @throws RuntimeException if an IOException occurs during closing
     */
    @Override
    public void close() {
        try {
            reader.close(); // Close the BufferedReader and underlying InputStream
        } catch (IOException e) {
            throw new RuntimeException("Error closing stream", e); // Handle I/O exception
        }
    }
}
