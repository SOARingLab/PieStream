package org.example.piepair.predicate;

import org.example.events.Attribute;
import org.example.events.PointEvent;



public class Greater implements Predicate {
    @Override
    public boolean test(PointEvent event, Attribute attribute, Object parameter) {
        Object attributeValue = event.getPayload().get(attribute);
        return PredicateUtils.compareValues(attributeValue, parameter, attribute.getType(), ">");
    }
}
