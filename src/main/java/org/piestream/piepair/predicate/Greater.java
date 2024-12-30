package org.piestream.piepair.predicate;

import org.piestream.events.Attribute;
import org.piestream.events.PointEvent;

/**
 * This class implements the Predicate interface to define a condition that tests if
 * the value of a specified attribute in a PointEvent is greater than a given parameter.
 * It uses the "greater than" (>) comparison to evaluate the condition.
 */
public class Greater implements Predicate {

    /**
     * Tests whether the value of the specified attribute in the given event is greater than
     * the provided parameter. The comparison is done based on the attribute's type.
     *
     * @param event The PointEvent that contains the payload (data) to be checked.
     * @param attribute The attribute of the event whose value is being compared.
     * @param parameter The value to compare the attribute's value against.
     * @return true if the value of the specified attribute in the event is greater than the parameter, false otherwise.
     */
    @Override
    public boolean test(PointEvent event, Attribute attribute, Object parameter) {
        Object attributeValue = event.getPayload().get(attribute);  // Retrieves the value of the attribute from the event's payload
        if (attributeValue == null) {
            throw new IllegalArgumentException(
                    String.format("Attribute '%s' value is null in the event payload.", attribute.getName())
            );
        }
        return PredicateUtils.compareValues(attributeValue, parameter, attribute.getType(), ">");  // Compares the values using the ">" operator
    }
}
