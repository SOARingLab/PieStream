package org.piestream.piepair.eba;

import org.piestream.events.Attribute;
import org.piestream.events.PointEvent;
import org.piestream.piepair.predicate.Predicate;

public class PredicateEBA extends EBA {
    private final Predicate predicate;
    private final Attribute attribute;
    private final Object parameter;

    public PredicateEBA(Predicate predicate, Attribute attribute, Object parameter) {
        this.predicate = predicate;
        this.attribute = attribute;
        this.parameter = parameter;
    }

    @Override
    public boolean evaluate(PointEvent event) {
        return predicate.test(event, attribute, parameter);
    }
}