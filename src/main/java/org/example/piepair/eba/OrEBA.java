package org.example.piepair.eba;

import org.example.events.PointEvent;

public class OrEBA extends  EBA{
    private final EBA left;
    private final EBA right;

    public OrEBA(EBA left, EBA right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public boolean evaluate(PointEvent event) {
        return left.evaluate(event) || right.evaluate(event);
    }
}
