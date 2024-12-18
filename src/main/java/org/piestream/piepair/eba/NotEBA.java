package org.piestream.piepair.eba;

import org.piestream.events.PointEvent;

/**
 * Represents a "NOT" (logical negation) EBA (Event-Based Automaton) that negates the evaluation of another EBA.
 * This class evaluates the expression and returns the opposite boolean value of the evaluation result.
 */
public class NotEBA extends EBA {

    // The EBA expression to be negated
    private final EBA expression;

    /**
     * Constructs a NotEBA with the specified EBA expression.
     *
     * @param expression The EBA expression to be negated.
     */
    public NotEBA(EBA expression) {
        this.expression = expression;
    }

    /**
     * Evaluates the negation of the expression for the given event.
     * Returns the opposite of the evaluation result of the expression.
     *
     * @param event The event to evaluate against the expression.
     * @return The negated evaluation result of the expression for the given event.
     */
    @Override
    public boolean evaluate(PointEvent event) {
        return !expression.evaluate(event);
    }
}
