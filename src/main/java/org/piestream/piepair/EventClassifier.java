package org.piestream.piepair;

import org.piestream.events.PointEvent;
import org.piestream.piepair.eba.EBA;
import org.piestream.piepair.dfa.Alphabet;

/**
 * Classifies a PointEvent into one of the alphabet categories based on the evaluation of two Event-Based Automatons (EBAs).
 * The classification is determined by evaluating two EBAs (formerPred and latterPred) and returning an appropriate alphabet symbol
 * based on their boolean evaluation results.
 */
public class EventClassifier {

    // The former predicate (EBA) used for classification
    private final EBA formerPred;

    // The latter predicate (EBA) used for classification
    private final EBA latterPred;

    /**
     * Constructs an EventClassifier with the specified former and latter predicates (EBAs).
     *
     * @param formerPred The former EBA used for classification.
     * @param latterPred The latter EBA used for classification.
     */
    public EventClassifier(EBA formerPred, EBA latterPred) {
        this.formerPred = formerPred;
        this.latterPred = latterPred;
    }

    /**
     * Classifies the given PointEvent into one of the alphabet categories (E, O, Z, I).
     *
     * The classification is based on the evaluation results of two EBAs:
     * - E: Both EBAs are true.
     * - O: Both EBAs are false.
     * - Z: The former EBA is true, and the latter EBA is false.
     * - I: The former EBA is false, and the latter EBA is true.
     *
     * @param event The event to be classified.
     * @return The corresponding alphabet symbol (E, O, Z, I).
     * @throws IllegalArgumentException If the provided event is null.
     */
    public Alphabet classify(PointEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("PointEvent cannot be null");
        }

        // Evaluate both EBAs with the event
        boolean formerResult = formerPred.evaluate(event);
        boolean latterResult = latterPred.evaluate(event);

        // Classify based on the evaluation results
        if (formerResult && latterResult) {
            return Alphabet.E; // Both FormerPred and LatterPred are true
        } else if (!formerResult && !latterResult) {
            return Alphabet.O; // Both FormerPred and LatterPred are false
        } else if (formerResult) {
            return Alphabet.Z; // FormerPred is true, LatterPred is false
        } else {
            return Alphabet.I; // FormerPred is false, LatterPred is true
        }
    }
}
