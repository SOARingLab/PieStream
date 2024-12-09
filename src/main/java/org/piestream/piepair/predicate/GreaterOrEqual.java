package org.piestream.piepair.predicate;

import org.piestream.events.Attribute;
import org.piestream.events.PointEvent;


public class GreaterOrEqual implements Predicate {
    @Override
    public boolean test(PointEvent event, Attribute attribute, Object parameter) {
        Object attributeValue = event.getPayload().get(attribute);
        return PredicateUtils.compareValues(attributeValue, parameter, attribute.getType(), ">=");
    }
}
