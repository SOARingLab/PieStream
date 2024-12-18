package org.piestream.engine;

import org.piestream.parser.Schema;

/**
 * The Window class represents a sliding window used in stream processing.
 * A window can be defined based on time (TIME_WINDOW) or event count (COUNT_WINDOW), and it has a capacity
 * that determines its size (either in time units or count of events).
 *
 * The window capacity can be expressed in terms of time units or event counts, and it is adjusted
 * based on the provided data stream unit (e.g., milliseconds, seconds, or event counts).
 */
public class Window {

    private WindowType windowType;    // The type of the window (TIME_WINDOW or COUNT_WINDOW)
    private long windowCapacity;      // The capacity of the window, in units (time or count)

    /**
     * Constructor for the Window class. Initializes the window type and its capacity.
     *
     * The capacity is specified in nanoseconds, and it is converted to the appropriate data stream unit
     * (e.g., milliseconds or event counts) by dividing the provided capacity by the data stream unit.
     *
     * @param windowType      The type of the window (TIME_WINDOW or COUNT_WINDOW)
     * @param windowCapacityNS The capacity of the window in nanoseconds
     * @param dataStreamUnitNS The unit of time or count in nanoseconds, used for conversion
     */
    public Window(WindowType windowType, long windowCapacityNS, long dataStreamUnitNS) {
        this.windowType = windowType;
        this.windowCapacity = windowCapacityNS / dataStreamUnitNS;  // Convert capacity to the appropriate unit
    }

    /**
     * Gets the type of the window.
     *
     * @return The window type (TIME_WINDOW or COUNT_WINDOW)
     */
    public WindowType getWindowType() {
        return windowType;
    }

    /**
     * Sets the type of the window.
     *
     * @param windowType The new window type (TIME_WINDOW or COUNT_WINDOW)
     */
    public void setWindowType(WindowType windowType) {
        this.windowType = windowType;
    }

    /**
     * Gets the capacity of the window.
     *
     * @return The window capacity, in units (time or count)
     */
    public long getWindowCapacity() {
        return windowCapacity;
    }

    /**
     * Sets the capacity of the window.
     *
     * @param windowCapacity The new window capacity, in units (time or count)
     */
    public void setWindowCapacity(long windowCapacity) {
        this.windowCapacity = windowCapacity;
    }

    /**
     * Returns a string representation of the Window object.
     *
     * @return A string that describes the Window object, including its type and capacity
     */
    @Override
    public String toString() {
        return "Window{" +
                "windowType=" + windowType +
                ", windowCapacity=" + windowCapacity +
                '}';
    }
}
