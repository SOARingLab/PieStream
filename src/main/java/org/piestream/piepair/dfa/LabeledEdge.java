package org.piestream.piepair.dfa;

import org.jgrapht.graph.DefaultEdge;

import java.io.Serializable;

/**
 * This class represents a labeled edge in a graph.
 * It extends the DefaultEdge class from JGraphT and adds a label to store a transition (trans).
 * The edge can be serialized for persistence or transmission.
 */
public class LabeledEdge extends DefaultEdge implements Serializable {
    private String trans;  // The transition label associated with the edge

    /**
     * Constructor to create a labeled edge with a given transition label.
     *
     * @param trans the transition label for this edge.
     */
    public LabeledEdge(String trans) {
        this.trans = trans;
    }

    /**
     * Gets the transition label of this edge.
     *
     * @return the transition label.
     */
    public String getTrans() {
        return trans;
    }

    /**
     * Returns a string representation of the edge, including the transition label.
     * This overrides the default toString method of DefaultEdge.
     *
     * @return a string representation of the edge in the format "Edge [trans=label]".
     */
    @Override
    public String toString() {
        return super.toString() + " [trans=" + trans + "]";
    }
}
