package org.piestream.piepair.dfa;

import org.jgrapht.graph.DefaultEdge;

import java.io.Serializable;

public class LabeledEdge extends DefaultEdge implements Serializable {
    private String trans;

    public LabeledEdge(String trans) {
        this.trans = trans;
    }

    public String getTrans() {
        return trans;
    }

    @Override
    public String toString() {
        return super.toString() + " [trans=" + trans + "]";
    }
}
