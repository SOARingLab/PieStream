package org.piestream.piepair.eba;

import org.piestream.events.PointEvent;

/**
 * This class represents a logical OR combination of two EBA (Event-Based Automata) objects.
 * It evaluates to true if either the left or right EBA evaluates to true for a given event.
 * This class extends the EBA base class and overrides the evaluate method to perform the OR operation.
 */
public class OrEBA extends EBA {
    private final EBA left;  // The left EBA for the OR operation
    private final EBA right; // The right EBA for the OR operation

    /**
     * Constructs an OrEBA with the specified left and right EBAs.
     *
     * @param left The left EBA to be evaluated in the OR operation.
     * @param right The right EBA to be evaluated in the OR operation.
     */
    public OrEBA(EBA left, EBA right) {
        this.left = left;
        this.right = right;
    }

    /**
     * Evaluates the OR of the left and right EBAs for the given event.
     *
     * @param event The event to be evaluated.
     * @return true if either the left or right EBA evaluates to true for the given event, false otherwise.
     */
    @Override
    public boolean evaluate(PointEvent event) {
        return left.evaluate(event) || right.evaluate(event);
    }
}
