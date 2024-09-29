package org.example.piepair.predicate;

import org.example.events.Attribute;
import org.example.events.PointEvent;

@FunctionalInterface
public interface Predicate {
    boolean test(PointEvent event, Attribute attribute, Object parameter );
}
