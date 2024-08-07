package org.example.piepair.eba.predicate;

import org.example.events.Attribute;
import org.example.events.PointEvent;
import org.example.events.Schema;

public class GreaterOrEqual implements Predicate {
    @Override
    public boolean test(PointEvent event, Attribute attribute, Object parameter, Schema schema) {
        String attributeValue = schema.getValue(attribute, event);
        try {
            double attributeDoubleValue = Double.parseDouble(attributeValue);
            double parameterDoubleValue = Double.parseDouble(parameter.toString());
            return attributeDoubleValue >= parameterDoubleValue;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
