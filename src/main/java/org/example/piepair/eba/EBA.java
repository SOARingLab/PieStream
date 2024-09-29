package org.example.piepair.eba;

import org.example.events.PointEvent;

public abstract class EBA {

    public abstract boolean evaluate(PointEvent event);
    // Custom ParseException
    public static class ParseException extends Exception {
        public ParseException(String message) {
            super(message);
        }
    }



}
