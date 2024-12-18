package org.piestream.piepair.predicate;

import org.piestream.events.Attribute;
import org.piestream.events.PointEvent;

/**
 * This functional interface represents a predicate (a condition or test) that can be applied to a
 * PointEvent based on a specific attribute and a parameter.
 * The test method evaluates whether the condition holds for the given event, attribute, and parameter.
 */
@FunctionalInterface
public interface Predicate {

    /**
     * Tests whether a condition is satisfied for the given PointEvent based on a specific attribute
     * and a parameter. The condition is determined by the implementation of the predicate.
     *
     * @param event The PointEvent containing the data (payload) to be checked.
     * @param attribute The attribute of the event to be evaluated.
     * @param parameter The value to compare against or use in the evaluation.
     * @return true if the condition is satisfied, false otherwise.
     */
    boolean test(PointEvent event, Attribute attribute, Object parameter);
}
