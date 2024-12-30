package org.piestream.piepair.predicate;

import org.piestream.events.Attribute;
import org.piestream.events.PointEvent;

/**
 * Represents a predicate that checks if the value of a specific attribute in a PointEvent is equal to a given parameter.
 * This predicate uses the equality operator (==) to compare the attribute's value with the parameter.
 */
public class Equals implements Predicate {

    /**
     * Evaluates whether the value of the specified attribute in the event is equal to the given parameter.
     * This comparison is done using the equality operator (==).
     *
     * @param event The event whose attribute is being evaluated.
     * @param attribute The attribute of the event whose value will be compared.
     * @param parameter The value to compare the attribute's value against.
     * @return True if the attribute's value equals the parameter, false otherwise.
     */
    @Override
    public boolean test(PointEvent event, Attribute attribute, Object parameter) {
        // Retrieve the value of the specified attribute from the event's payload
        Object attributeValue = event.getPayload().get(attribute);

        if (attributeValue == null) {
            throw new IllegalArgumentException(
                    String.format("Attribute '%s' value is null in the event payload.", attribute.getName())
            );
        }
        // Compare the attribute's value to the parameter using the specified comparison operator (==)
        return PredicateUtils.compareValues(attributeValue, parameter, attribute.getType(), "==");
    }
}
