package org.example.piepair.eba.predicate;

import org.example.events.Attribute;
import org.example.events.PointEvent;
import org.example.events.Schema;

@FunctionalInterface
public interface Predicate {
    boolean test(PointEvent event, Attribute attribute, Object parameter );
}
