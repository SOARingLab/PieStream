package org.piestream.piepair.aggfuncs;

/**
 * Implementation of the "Count" aggregation function.
 * This function counts the number of events or values processed.
 * It is used to track the number of occurrences of an event or item in a stream.
 */
public class Count implements AggregationFunction<Long> {

    private long count = 0; // Counter to keep track of the number of updates (events)

    /**
     * Updates the count each time a new value is processed.
     * Increments the counter by 1 for each new event.
     *
     * @param value The value to be processed (not used in count operation, only increments the counter).
     */
    @Override
    public void update(Long value) {
        count++; // Increment the count each time an event arrives
    }

    /**
     * Returns the current count of processed events.
     *
     * @return The current count as a Long value.
     */
    @Override
    public Long getResult() {
        return count; // Return the current count of events
    }

    /**
     * Resets the count back to zero. This is useful when you need to start a new aggregation.
     */
    @Override
    public void reset() {
        count = 0; // Reset the counter to zero
    }
}
