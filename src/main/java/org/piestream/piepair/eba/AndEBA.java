package org.piestream.piepair.eba;

import org.piestream.events.PointEvent;

/**
 * Represents an "AND" (logical conjunction) EBA (Event-Based Automaton) that evaluates two other EBAs.
 * This class combines two EBAs using a logical AND operation: it returns true if both the left and right EBAs evaluate to true for a given event.
 */
public class AndEBA extends EBA {

    // The left EBA operand in the AND operation
    private final EBA left;

    // The right EBA operand in the AND operation
    private final EBA right;

    /**
     * Constructs an AndEBA with the specified left and right EBAs.
     *
     * @param left The left EBA in the AND operation.
     * @param right The right EBA in the AND operation.
     */
    public AndEBA(EBA left, EBA right) {
        this.left = left;
        this.right = right;
    }

    /**
     * Evaluates the "AND" of the left and right EBAs for the given event.
     * Returns true if both the left and right EBAs evaluate to true for the event, otherwise returns false.
     *
     * @param event The event to evaluate against the left and right EBAs.
     * @return True if both the left and right EBAs evaluate to true for the given event, false otherwise.
     */
    @Override
    public boolean evaluate(PointEvent event) {
        return left.evaluate(event) && right.evaluate(event);
    }
}
