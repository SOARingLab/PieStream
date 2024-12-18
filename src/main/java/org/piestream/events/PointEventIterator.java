package org.piestream.events;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class PointEventIterator implements Iterator<PointEvent> {
    // List to store the PointEvent objects
    private final List<PointEvent> events;

    // Index to track the current position in the event list
    private int currentIndex;

    /**
     * Constructor to initialize the event list and the current index.
     */
    public PointEventIterator() {
        this.events = new LinkedList<>();
        this.currentIndex = 0;
    }

    /**
     * Adds a new PointEvent to the event list.
     *
     * @param event The PointEvent object to be added to the list
     */
    public void addEvent(PointEvent event) {
        events.add(event);
    }

    /**
     * Checks if there are more events to iterate over.
     *
     * @return true if there are more events, false otherwise
     */
    @Override
    public boolean hasNext() {
        return currentIndex < events.size();
    }

    /**
     * Retrieves the next event in the list.
     *
     * @return The next PointEvent object
     * @throws NoSuchElementException if there are no more events to return
     */
    @Override
    public PointEvent next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more events");
        }
        return events.get(currentIndex++);
    }

    /**
     * Removes the current event from the list.
     *
     * @throws IllegalStateException if the iterator is in an invalid state for removal
     */
    @Override
    public void remove() {
        if (currentIndex <= 0 || currentIndex > events.size()) {
            throw new IllegalStateException("Invalid state for removal");
        }
        events.remove(--currentIndex);
    }

    /**
     * Resets the iterator to the beginning of the event list.
     */
    public void reset() {
        currentIndex = 0;
    }

    /**
     * Retrieves the number of events in the list.
     *
     * @return The size of the event list
     */
    public int size() {
        return events.size();
    }
}
