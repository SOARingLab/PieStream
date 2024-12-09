package org.piestream.piepair.predicate;

import org.piestream.events.Attribute;
import org.piestream.events.PointEvent;

@FunctionalInterface
public interface Predicate {
    boolean test(PointEvent event, Attribute attribute, Object parameter );
}
