package org.piestream.piepair;

import org.piestream.events.Expirable;
import org.piestream.events.PointEvent;
import org.piestream.piepair.eba.EBA;

import java.util.Objects;

/**
 * This class represents an "Interaction Event" (IE), which encapsulates an event triggered by a condition
 * defined by an Event-Based Automaton (EBA). It tracks a start event, an optional end event,
 * and associated timestamps for these events along with the trigger time.
 * This class implements the Expirable interface, allowing it to determine if it has expired based on a deadline.
 */
public class IE implements Expirable {

    private final EBA pred;            // The Event-Based Automaton (EBA) predicate associated with this interaction event
    private final PointEvent startEvent;    // The start event of the interaction
    private PointEvent endEvent;      // The end event of the interaction (can be null)
    private final Long triggerTime;   // The time when the interaction event was triggered
    private final Long startTime;     // The timestamp of the start event
    private Long endTime;             // The timestamp of the end event (can be null)

    /**
     * Constructs an IE (Interaction Event) with the specified parameters.
     *
     * @param pred The EBA predicate associated with this interaction event.
     * @param startEvent The start event of the interaction, which cannot be null.
     * @param endEvent The end event of the interaction, which can be null.
     * @param triggerTime The timestamp when the interaction event was triggered.
     */
    public IE(EBA pred, PointEvent startEvent, PointEvent endEvent, long triggerTime) {
        this.pred = pred;
        this.startEvent = Objects.requireNonNull(startEvent, "startEvent cannot be null");  // Ensures startEvent is not null
        this.endEvent = endEvent;
        this.startTime = Objects.requireNonNull(startEvent.getTimestamp(), "startEvent timestamp cannot be null");  // Ensures the start event has a timestamp
        this.endTime = (endEvent != null) ? endEvent.getTimestamp() : null;  // Sets endTime to null if endEvent is null
        this.triggerTime = triggerTime;
    }

    // Getter methods

    /**
     * @return The EBA predicate associated with this interaction event.
     */
    public EBA getPred() {
        return pred;
    }

    /**
     * @return The start event of the interaction.
     */
    public PointEvent getStartEvent() {
        return startEvent;
    }

    /**
     * @return The end event of the interaction, or null if no end event is set.
     */
    public PointEvent getEndEvent() {
        return endEvent;
    }

    /**
     * Sets the end event for this interaction.
     *
     * @param endEvent The new end event to be set.
     */
    public void setEndEvent(PointEvent endEvent) {
        this.endEvent = endEvent;
    }

    /**
     * @return The timestamp of the start event.
     */
    public Long getStartTime() {
        return startTime;
    }

    /**
     * @return The timestamp of the end event, or null if the end event is not set.
     */
    public Long getEndTime() {
        return endTime;
    }

    /**
     * Sets the timestamp of the end event.
     *
     * @param endTime The timestamp to be set for the end event.
     */
    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    /**
     * Checks whether this interaction event has expired based on the provided deadline.
     *
     * @param deadLine The deadline against which the expiration of the event is checked.
     * @return true if the event has expired (i.e., the start time is before the deadline), false otherwise.
     */
    @Override
    public boolean isExpired(long deadLine) {
        return startTime < deadLine;  // Expiration check is based on the start time
    }

    /**
     * Returns the sort key for the interaction event, which is its trigger time.
     *
     * @return The trigger time, used for sorting the interaction event.
     */
    @Override
    public long getSortKey() {
        return triggerTime;  // Trigger time is used as the sort key
    }
}
