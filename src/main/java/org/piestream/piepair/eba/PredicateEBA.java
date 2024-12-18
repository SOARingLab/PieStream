package org.piestream.piepair.eba;

import org.piestream.events.Attribute;
import org.piestream.events.PointEvent;
import org.piestream.piepair.predicate.Predicate;

/**
 * Represents an EBA (Event-Based Automaton) that evaluates an event based on a predicate applied to an attribute.
 * The predicate is tested on a specific attribute of the event with a parameter.
 */
public class PredicateEBA extends EBA {

    // The predicate to apply to the event's attribute
    private final Predicate predicate;

    // The attribute of the event to evaluate with the predicate
    private final Attribute attribute;

    // The parameter to be used with the predicate
    private final Object parameter;

    /**
     * Constructs a PredicateEBA with the specified predicate, attribute, and parameter.
     *
     * @param predicate The predicate to apply to the event's attribute.
     * @param attribute The attribute of the event to evaluate.
     * @param parameter The parameter to be used with the predicate.
     */
    public PredicateEBA(Predicate predicate, Attribute attribute, Object parameter) {
        this.predicate = predicate;
        this.attribute = attribute;
        this.parameter = parameter;
    }

    /**
     * Evaluates the event by applying the predicate to the event's attribute using the specified parameter.
     *
     * @param event The event to evaluate.
     * @return The result of applying the predicate to the event's attribute with the parameter.
     */
    @Override
    public boolean evaluate(PointEvent event) {
        return predicate.test(event, attribute, parameter);
    }
}
