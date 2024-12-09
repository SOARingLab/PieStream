package org.piestream.piepair.eba;

import org.piestream.events.PointEvent;

public abstract class EBA {

    public abstract boolean evaluate(PointEvent event);
    // Custom ParseException
    public static class ParseException extends Exception {
        public ParseException(String message) {
            super(message);
        }
    }



}
