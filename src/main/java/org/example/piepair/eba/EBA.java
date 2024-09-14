package org.example.piepair.eba;

import org.example.events.Attribute;
import org.example.events.PointEvent;
import org.example.piepair.predicate.Predicate;
import org.example.piepair.predicate.PredicateFactory;

import java.util.Stack;

public abstract class EBA {

    public abstract boolean evaluate(PointEvent event);
    // Custom ParseException
    public static class ParseException extends Exception {
        public ParseException(String message) {
            super(message);
        }
    }



}
