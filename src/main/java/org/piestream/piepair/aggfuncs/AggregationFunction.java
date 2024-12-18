package org.piestream.piepair.aggfuncs;

/**
 * This interface defines the contract for aggregation functions.
 * It provides methods to update the state of the aggregator, retrieve the current aggregation result,
 * and reset the aggregator to its initial state.
 *
 * @param <T> the type of the value being aggregated.
 */
public interface AggregationFunction<T> {

    /**
     * Updates the state of the aggregator with the provided value.
     * This method is typically called whenever a new event arrives.
     *
     * @param value the new value to update the aggregation state with.
     */
    void update(T value);

    /**
     * Retrieves the current result of the aggregation.
     *
     * @return the current aggregation result.
     */
    T getResult();

    /**
     * Resets the state of the aggregator, clearing any accumulated data.
     * This method can be called when the aggregation needs to start over.
     */
    void reset();
}
