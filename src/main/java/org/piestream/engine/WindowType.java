package org.piestream.engine;

/**
 * The WindowType enum defines the types of windows used in stream processing.
 * It supports two types of windows:
 *
 *     TIME_WINDOW: A window defined by a specific time duration.
 *     CAPACITY_WINDOW: A window defined by the number of events or capacity of a data structure.
 *
 * This enum helps in managing and distinguishing between different windowing strategies in the stream processing system.
 */
public enum WindowType {
    TIME_WINDOW,  // Time-based window, defined by a specific time duration.
    CAPACITY_WINDOW; // Count-based window, defined by the number of events or capacity of a data structure.

    /**
     * Converts the WindowType enum value to a human-readable string.
     *
     * @return A string representation of the WindowType.
     *         - "Time_Window" for TIME_WINDOW
     *         - "Count_Window" for CAPACITY_WINDOW (Note: not recommended for window managed by data structure capacity)
     * @throws IllegalArgumentException If the WindowType is unknown.
     */
    @Override
    public String toString() {
        switch (this) {
            case TIME_WINDOW:
                return "Time_Window";  // Human-readable string for TIME_WINDOW
            case CAPACITY_WINDOW:
                return "Count_Window";  // Human-readable string for CAPACITY_WINDOW
            default:
                throw new IllegalArgumentException("Unknown WindowType: " + this);  // Handle unexpected enum values
        }
    }
}
