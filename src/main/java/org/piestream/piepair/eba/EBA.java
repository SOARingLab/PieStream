package org.piestream.piepair.eba;

import org.piestream.events.PointEvent;

/**
 * This abstract class represents an Event-Based Analysis (EBA) evaluator.
 * It defines the structure for evaluating events and includes an exception for parsing errors.
 */
public abstract class EBA {

    /**
     * Evaluates the given event based on the specific logic of the subclass.
     *
     * @param event the event to be evaluated.
     * @return true if the evaluation condition is met, false otherwise.
     */
    public abstract boolean evaluate(PointEvent event);

    /**
     * Custom exception class for parsing errors.
     * This exception is thrown when a parsing error occurs within the EBA evaluation process.
     */
    public static class ParseException extends Exception {

        /**
         * Constructor to create a new ParseException with a custom error message.
         *
         * @param message the error message describing the parse issue.
         */
        public ParseException(String message) {
            super(message);
        }
    }
}
