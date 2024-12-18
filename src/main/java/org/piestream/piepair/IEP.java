package org.piestream.piepair;

import java.util.Objects;

import org.piestream.events.Expirable;
import org.piestream.events.PointEvent;
import org.piestream.piepair.eba.EBA;

/**
 * The IEP class represents an Interval Event Pair, encapsulating the relationship between two events
 * with their respective start and end points, temporal relations, and trigger information.
 * It implements the Expirable interface to determine if the event pair has expired based on a deadline.
 */
public class IEP implements Expirable {
    private final EBA formerPie;
    private final EBA latterPie;
    private final TemporalRelations.AllRel relation; /* Temporal relation */
    private final PointEvent formerPieStart;         /* Start event of the former PIE */
    private final PointEvent latterPieStart;         /* Start event of the latter PIE (can be null) */
    private PointEvent formerPieEnd;           /* End event of the former PIE */
    private PointEvent latterPieEnd;           /* End event of the latter PIE (can be null) */
    private Long formerStartTime;              /* Start time of the former event */
    private Long latterStartTime;              /* Start time of the latter event */
    private final PointEvent triggerEvent;
    private final Long triggerTime;
    private final Long systemTriggerTime;
    private final CompletedTime compTime;

    /**
     * Enum representing the completion time of the IEP.
     */
    public enum CompletedTime {
        FormerEnd,
        LatterEnd,
        NoNeed
    }

    /**
     * Constructs an IEP instance with the specified temporal relation, EBAs, events, and trigger information.
     *
     * @param relation         The temporal relation defining the relationship between events (AllRel)
     * @param formerPie        The former Event-Based Attribute (EBA)
     * @param latterPie        The latter Event-Based Attribute (EBA)
     * @param formerPieStart   The start event of the former PIE
     * @param latterPieStart   The start event of the latter PIE
     * @param formerPieEnd     The end event of the former PIE
     * @param latterPieEnd     The end event of the latter PIE
     * @param formerStartTime  The start time of the former event
     * @param latterStartTime  The start time of the latter event
     * @param triggerEvent     The event that triggers this IEP
     * @param triggerTime      The time when the trigger event occurs
     */
    public IEP(TemporalRelations.AllRel relation,
               EBA formerPie,
               EBA latterPie,
               PointEvent formerPieStart,
               PointEvent latterPieStart,
               PointEvent formerPieEnd,
               PointEvent latterPieEnd,
               Long formerStartTime,
               Long latterStartTime,
               PointEvent triggerEvent,
               Long triggerTime) {

        /* Validate non-null parameters */
        this.relation = Objects.requireNonNull(relation, "relation cannot be null");
        this.formerPieStart = Objects.requireNonNull(formerPieStart, "formerPieStart cannot be null");
        this.latterPieStart = Objects.requireNonNull(latterPieStart, "latterPieStart cannot be null");
        this.formerStartTime = Objects.requireNonNull(formerStartTime, "formerStartTime cannot be null");
        this.latterStartTime = Objects.requireNonNull(latterStartTime, "latterStartTime cannot be null");
        this.triggerEvent = Objects.requireNonNull(triggerEvent, "triggerEvent cannot be null");
        this.triggerTime = Objects.requireNonNull(triggerTime, "triggerTime cannot be null");

        /* Allow formerPieEnd and latterPieEnd to be null */
        this.formerPieEnd = formerPieEnd;
        this.latterPieEnd = latterPieEnd;
        this.formerPie = formerPie;
        this.latterPie = latterPie;
        this.compTime = determinCompTimeByRel();
        this.systemTriggerTime = System.nanoTime();

        // this.isCompleted = false;
    }

    /**
     * Constructs an IEP instance from a precise temporal relation.
     *
     * @param relation         The precise temporal relation (PreciseRel)
     * @param formerPie        The former Event-Based Attribute (EBA)
     * @param latterPie        The latter Event-Based Attribute (EBA)
     * @param formerPieStart   The start event of the former PIE
     * @param latterPieStart   The start event of the latter PIE
     * @param formerPieEnd     The end event of the former PIE
     * @param latterPieEnd     The end event of the latter PIE
     * @param formerStartTime  The start time of the former event
     * @param latterStartTime  The start time of the latter event
     * @param triggerEvent     The event that triggers this IEP
     * @param triggerTime      The time when the trigger event occurs
     */
    public IEP(TemporalRelations.PreciseRel relation,
               EBA formerPie,
               EBA latterPie,
               PointEvent formerPieStart,
               PointEvent latterPieStart,
               PointEvent formerPieEnd,
               PointEvent latterPieEnd,
               Long formerStartTime,
               Long latterStartTime,
               PointEvent triggerEvent,
               Long triggerTime) {

        /* Delegate to the main constructor after converting PreciseRel to AllRel */
        this(TemporalRelations.AllRel.fromPreciseRel(relation), formerPie, latterPie, formerPieStart, latterPieStart,
                formerPieEnd, latterPieEnd, formerStartTime, latterStartTime, triggerEvent, triggerTime);
    }

    /**
     * Constructs an IEP instance from an Allen temporal relation.
     *
     * @param relation         The Allen temporal relation (AllenRel)
     * @param formerPie        The former Event-Based Attribute (EBA)
     * @param latterPie        The latter Event-Based Attribute (EBA)
     * @param formerPieStart   The start event of the former PIE
     * @param latterPieStart   The start event of the latter PIE
     * @param formerPieEnd     The end event of the former PIE
     * @param latterPieEnd     The end event of the latter PIE
     * @param formerStartTime  The start time of the former event
     * @param latterStartTime  The start time of the latter event
     * @param triggerEvent     The event that triggers this IEP
     * @param triggerTime      The time when the trigger event occurs
     */
    public IEP(TemporalRelations.AllenRel relation,
               EBA formerPie,
               EBA latterPie,
               PointEvent formerPieStart,
               PointEvent latterPieStart,
               PointEvent formerPieEnd,
               PointEvent latterPieEnd,
               Long formerStartTime,
               Long latterStartTime,
               PointEvent triggerEvent,
               Long triggerTime) {

        /* Delegate to the main constructor after converting AllenRel to AllRel */
        this(TemporalRelations.AllRel.fromAllenRel(relation), formerPie, latterPie, formerPieStart, latterPieStart,
                formerPieEnd, latterPieEnd, formerStartTime, latterStartTime, triggerEvent, triggerTime);
    }
    /**
     * Retrieves the former Event-Based Attribute (EBA).
     *
     * @return the former EBA.
     */
    public EBA getFormerPie() {
        return formerPie;
    }

    /**
     * Retrieves the latter Event-Based Attribute (EBA).
     *
     * @return the latter EBA.
     */
    public EBA getLatterPie() {
        return latterPie;
    }

    /**
     * Retrieves the temporal relation of this IEP.
     *
     * @return the temporal relation (AllRel).
     */
    public TemporalRelations.AllRel getRelation() {
        return relation;
    }

    /**
     * Retrieves the system trigger time in nanoseconds.
     *
     * @return the system trigger time.
     */
    public Long getSystemTriggerTime() {
        return systemTriggerTime;
    }

    /**
     * Retrieves the start event of the former PIE.
     *
     * @return the former PIE start event.
     */
    public PointEvent getFormerPieStart() {
        return formerPieStart;
    }

    /**
     * Retrieves the completion time of this IEP.
     *
     * @return the completion time (CompletedTime).
     */
    public CompletedTime getCompTime() {
        return compTime;
    }

    /**
     * Retrieves the start event of the latter PIE.
     *
     * @return the latter PIE start event.
     */
    public PointEvent getLatterPieStart() {
        return latterPieStart;
    }

    /**
     * Retrieves the end event of the former PIE.
     *
     * @return the former PIE end event.
     */
    public PointEvent getFormerPieEnd() {
        return formerPieEnd;
    }

    /**
     * Sets the end event of the former PIE.
     *
     * @param formerPieEnd the former PIE end event to set.
     */
    public void setFormerPieEnd(PointEvent formerPieEnd) {
        this.formerPieEnd = formerPieEnd;
    }

    /**
     * Retrieves the end event of the latter PIE.
     *
     * @return the latter PIE end event.
     */
    public PointEvent getLatterPieEnd() {
        return latterPieEnd;
    }

    /**
     * Sets the end event of the latter PIE.
     *
     * @param latterPieEnd the latter PIE end event to set.
     */
    public void setLatterPieEnd(PointEvent latterPieEnd) {
        this.latterPieEnd = latterPieEnd;
    }

    /**
     * Sets the start time of the former event.
     *
     * @param formerStartTime the former event start time to set.
     */
    public void setFormerStartTime(Long formerStartTime) {
        this.formerStartTime = formerStartTime;
    }

    /**
     * Retrieves the start time of the former event.
     *
     * @return the former event start time.
     */
    public Long getFormerStartTime() {
        return formerStartTime;
    }

    /**
     * Retrieves the start time of the latter event.
     *
     * @return the latter event start time.
     */
    public Long getLatterStartTime() {
        return latterStartTime;
    }

    /**
     * Retrieves the end time of the former PIE.
     *
     * @return the former PIE end time, or 0L if not finished.
     */
    public Long getFormerEndTime() {
        if (formerPieEnd == null) {
            return 0L;
        } else {
            return formerPieEnd.getTimestamp();
        }
    }

    /**
     * Retrieves the end time of the latter PIE.
     *
     * @return the latter PIE end time, or 0L if not finished.
     */
    public Long getLatterEndTime() {
        if (latterPieEnd == null) {
            return 0L;
        } else {
            return latterPieEnd.getTimestamp();
        }
    }

    /**
     * Sets the start time of the latter event.
     *
     * @param latterStartTime the latter event start time to set.
     */
    public void setLatterStartTime(Long latterStartTime) {
        this.latterStartTime = latterStartTime;
    }

    /**
     * Returns a string representation of the IEP.
     *
     * @return a string describing the IEP.
     */
    @Override
    public String toString() {
        String formerPieEndTime = formerPieEnd == null ? "not finish" : String.valueOf(formerPieEnd.getTimestamp());
        String latterPieEndTime = latterPieEnd == null ? "not finish" : String.valueOf(latterPieEnd.getTimestamp());
        return "IEP{" +
                relation +
                ", " + formerPieStart.getTimestamp() +
                ", " + latterPieStart.getTimestamp() +
                ", " + formerPieEndTime +
                ", " + latterPieEndTime +
                '}';
    }

    /**
     * Computes the hash code for this IEP.
     *
     * @return the hash code.
     */
    @Override
    public int hashCode() {
        return Objects.hash(relation, formerPieStart, latterPieStart, formerPieEnd, latterPieEnd, formerStartTime, latterStartTime);
    }

    /**
     * Determines the completion time based on the temporal relation.
     *
     * @return the completion time (CompletedTime).
     */
    private CompletedTime determinCompTimeByRel() {
        if (relation.isPreciseRel()) {
            TemporalRelations.PreciseRel preRel = relation.getPreciseRel();
            if (preRel.triggerWithoutLatterPieEnd()) {
                return CompletedTime.LatterEnd;
            } else if (preRel.triggerWithoutFormerPieEnd()) {
                return CompletedTime.FormerEnd;
            } else if (preRel.triggerWithCompleted()) {
                return CompletedTime.NoNeed;
            } else {
                throw new IllegalStateException("Unexpected precise relation state.");
            }
        } else {
            if (relation.getAllenRel() == TemporalRelations.AllenRel.AFTER) {
                return CompletedTime.FormerEnd;
            } else if (relation.getAllenRel() == TemporalRelations.AllenRel.BEFORE) {
                return CompletedTime.LatterEnd;
            } else {
                throw new IllegalStateException("Unexpected precise relation state.");
            }
        }
    }

    /**
     * Determines if this IEP is equal to another object.
     *
     * @param o the object to compare with.
     * @return true if equal, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IEP iep = (IEP) o;

        /* If hash codes are not equal, objects are not equal */
        if (this.hashCode() != iep.hashCode()) {
            return false;
        }

        /* Compare all relevant fields for equality */
        return relation == iep.relation &&
                Objects.equals(formerPieStart, iep.formerPieStart) &&
                Objects.equals(latterPieStart, iep.latterPieStart) &&
                Objects.equals(formerPieEnd, iep.formerPieEnd) &&
                Objects.equals(latterPieEnd, iep.latterPieEnd) &&
                Objects.equals(formerStartTime, iep.formerStartTime) &&
                Objects.equals(latterStartTime, iep.latterStartTime);
    }

    /**
     * Retrieves the trigger time of this IEP.
     *
     * @return the trigger time.
     */
    public Long getTriggerTime() {
        return triggerTime;
    }

    /**
     * Retrieves the start time based on the provided EBA predicate.
     *
     * @param pred the EBA predicate (formerPie or latterPie).
     * @return the corresponding start time.
     * @throws IllegalArgumentException if the predicate does not match formerPie or latterPie.
     */
    public Long getStartTime(EBA pred) {
        if (pred == formerPie) {
            return formerStartTime;
        } else if (pred == latterPie) {
            return latterStartTime;
        } else {
            throw new IllegalArgumentException("The provided EBA predicate does not match formerPie or latterPie.");
        }
    }

    public  boolean isExpired(long deadLine){
        return formerStartTime<=latterStartTime?formerStartTime<deadLine:latterStartTime<deadLine;
    }
    @Override
    public long getSortKey() {
        return triggerTime;
    }
}