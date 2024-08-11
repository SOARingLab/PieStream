package org.example.piepair;

import org.example.events.PointEvent;
import org.example.piepair.eba.EBA;
import org.example.piepair.dfa.Alphabet;

public class EventClassifier {
    private final EBA formerPred;
    private final EBA latterPred;

    public EventClassifier(EBA formerPred, EBA latterPred) {
        this.formerPred = formerPred;
        this.latterPred = latterPred;
    }

    public Alphabet classify(PointEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("PointEvent cannot be null");
        }

        boolean formerResult = formerPred.evaluate(event);
        boolean latterResult = latterPred.evaluate(event);

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
