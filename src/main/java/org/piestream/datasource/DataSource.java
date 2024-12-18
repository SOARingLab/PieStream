package org.piestream.datasource;

import java.util.List;

/**
 * Interface for data sources that provide a stream of records for processing.
 * This interface defines methods for reading individual records, checking if more records are available,
 * reading records in batches, and closing the data source when finished.
 * It is intended to be implemented by various data source classes, such as those that handle binary files,
 * databases, or other event data sources.
 */
public interface DataSource extends AutoCloseable {

    /**
     * Reads the next record from the data source.
     * The record is typically returned in a string format, which can be processed by the application.
     *
     * @return the next record as a string, or null if there are no more records
     */
    String readNext();

    /**
     * Checks if there are more records to read from the data source.
     * This method allows clients to check whether the data source has more data available.
     *
     * @return true if there are more records, false otherwise
     */
    boolean hasNext();

    /**
     * Reads a batch of records from the data source.
     * This method is useful when processing data in bulk or for performance optimizations.
     *
     * @param batchSize the number of records to read in the batch
     * @return a list of records as strings
     */
    List<String> readBatch(int batchSize);

    /**
     * Closes the data source and releases any resources associated with it.
     * This method should be called when the data source is no longer needed.
     */
    void close();
}
