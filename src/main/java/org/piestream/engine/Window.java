package org.piestream.engine;


import org.piestream.parser.Schema;

public class Window {

    private WindowType windowType;    // The type of the window (TIME_WINDOW or COUNT_WINDOW)
    private long windowCapacity;       // The capacity of the window (could be in time units or count)

    // Constructor
    public Window(WindowType windowType, long windowCapacityNS, long dataStreamUnitNS) {
        this.windowType = windowType;
        this.windowCapacity = windowCapacityNS/dataStreamUnitNS;
    }

    // Getter for windowType
    public WindowType getWindowType() {
        return windowType;
    }

    // Setter for windowType
    public void setWindowType(WindowType windowType) {
        this.windowType = windowType;
    }

    // Getter for windowCapacity
    public long getWindowCapacity() {
        return windowCapacity;
    }

    // Setter for windowCapacity
    public void setWindowCapacity(long windowCapacity) {
        this.windowCapacity = windowCapacity;
    }

    @Override
    public String toString() {
        return "Window{" +
                "windowType=" + windowType +
                ", windowCapacity=" + windowCapacity +
                '}';
    }
}
