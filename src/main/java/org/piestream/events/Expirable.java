package org.piestream.events;

public interface Expirable {

    /**
     * Checks if the object has expired.
     * This method should return true if the object is considered expired
     * based on the provided deadline.
     *
     * @param deadLine The deadline timestamp to compare against
     * @return True if the object is expired, false otherwise
     */
    boolean isExpired(long deadLine);

    /**
     * Retrieves a sorting key for the object.
     * This method can be used to sort or prioritize objects based on
     * a specific attribute or criteria.
     *
     * @return A sorting key (typically a long value)
     */
    long getSortKey();
}
