package org.piestream.datasource;

import com.google.common.util.concurrent.RateLimiter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link DataSource} implementation that reads data from a file.
 * Supports reading data with optional rate limiting and an optional limit on the number of lines to be read.
 */
public class FileDataSource implements DataSource {
    private BufferedReader reader;
    private long limit; // The maximum number of lines to read from the file
    private long readCount; // The number of lines that have been read so far
    private RateLimiter rateLimiter; // Rate limiter for controlling the read speed

    /**
     * Constructs a FileDataSource that reads from the specified file.
     * No limit is imposed on the number of lines to read by default.
     *
     * @param filePath the path to the file to read from
     * @throws IOException if an error occurs while opening the file
     */
    public FileDataSource(String filePath) throws IOException {
        reader = new BufferedReader(new FileReader(filePath));
        this.limit = Long.MAX_VALUE; // Default to no limit on the number of lines
        this.readCount = 0; // Initialize the read count
    }

    /**
     * Constructs a FileDataSource with a limit on the number of lines to read.
     *
     * @param filePath the path to the file to read from
     * @param limit the maximum number of lines to read
     * @throws IOException if an error occurs while opening the file
     */
    public FileDataSource(String filePath, long limit) throws IOException {
        reader = new BufferedReader(new FileReader(filePath));
        this.limit = limit;
        this.readCount = 0; // Initialize the read count
    }

    /**
     * Constructs a FileDataSource with a limit on the number of lines to read and a rate limit on reading speed.
     * The rate limiting controls how fast data is read from the file.
     *
     * @param filePath the path to the file to read from
     * @param limit the maximum number of lines to read
     * @param rates the rate limit (in number of reads per second), 0 or negative values disable rate limiting
     * @throws IOException if an error occurs while opening the file
     */
    public FileDataSource(String filePath, long limit, long rates) throws IOException {
        reader = new BufferedReader(new FileReader(filePath));
        this.limit = limit;
        this.readCount = 0; // Initialize the read count
        if (rates > 0) {
            this.rateLimiter = RateLimiter.create(rates); // Initialize the rate limiter
        } else {
            this.rateLimiter = null; // No rate limiting if rates <= 0
        }
    }

    /**
     * Checks if there are more lines to read from the file.
     * If the number of lines read exceeds the limit, returns false.
     *
     * @return true if there are more lines to read, false otherwise
     */
    @Override
    public boolean hasNext() {
        try {
            if (readCount >= limit) {
                return false; // No more lines to read if limit is reached
            }
            reader.mark(1); // Mark the current position
            if (reader.readLine() == null) {
                return false; // No more content in the file
            }
            reader.reset(); // Reset to the marked position
            return true; // More lines are available
        } catch (IOException e) {
            throw new RuntimeException("Error reading from file", e);
        }
    }

    /**
     * Reads the next line from the file.
     * Applies rate limiting if enabled, and stops if the read limit is exceeded.
     *
     * @return the next line of the file, or null if there are no more lines to read
     */
    @Override
    public String readNext() {
        try {
            if (readCount >= limit) {
                return null; // Stop reading if the limit is reached
            }
            if (rateLimiter != null) {
                rateLimiter.acquire(); // Apply rate limiting if enabled
            }
            String line = reader.readLine();
            if (line != null) {
                readCount++; // Increment the read count
            }
            return line;
        } catch (IOException e) {
            throw new RuntimeException("Error reading from file", e);
        }
    }

    /**
     * Reads a batch of lines from the file.
     * Returns a list of lines, up to the specified batch size.
     * Stops reading if the read limit is exceeded or if there are no more lines.
     *
     * @param batchSize the number of lines to read in the batch
     * @return a list of lines read from the file
     */
    @Override
    public List<String> readBatch(int batchSize) {
        List<String> batch = new ArrayList<>();
        try {
            for (int i = 0; i < batchSize; i++) {
                if (readCount >= limit) {
                    break; // Stop reading if the limit is reached
                }
                if (rateLimiter != null) {
                    rateLimiter.acquire(); // Apply rate limiting if enabled
                }
                String line = reader.readLine();
                if (line != null) {
                    batch.add(line);
                    readCount++; // Increment the read count
                } else {
                    break; // Stop if there are no more lines
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading from file", e);
        }
        return batch;
    }

    /**
     * Closes the file reader.
     * Ensures that the resources are properly released when done reading.
     */
    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing file", e);
        }
    }
}
