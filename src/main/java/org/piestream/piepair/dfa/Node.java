package org.piestream.piepair.dfa;

import java.io.Serializable;

/**
 * Represents a state node in a finite state machine (FSM).
 * This class encapsulates the name and the final state information of the node.
 * It implements {@link Serializable} to allow for object serialization.
 */
public class Node implements Serializable {

    // The name of the node
    private String name;

    // Flag indicating whether this node is a final state in the FSM
    private boolean isFinalState;

    /**
     * Constructs a Node with the specified name and final state flag.
     *
     * @param name The name of the node.
     * @param isFinalState Indicates whether the node is a final state in the FSM.
     */
    public Node(String name, boolean isFinalState) {
        this.name = name;
        this.isFinalState = isFinalState;
    }

    /**
     * Returns the name of the node.
     *
     * @return The name of the node.
     */
    public String getName() {
        return name;
    }

    /**
     * Checks if the node is a final state in the FSM.
     *
     * @return True if the node is a final state, false otherwise.
     */
    public boolean isFinalState() {
        return isFinalState;
    }

    /**
     * Sets the final state flag for the node.
     *
     * @param finalState The flag indicating whether the node is a final state.
     */
    public void setFinalState(boolean finalState) {
        isFinalState = finalState;
    }

    /**
     * Returns a string representation of the node, including the name and whether it is a final state.
     *
     * @return A string representation of the node.
     */
    @Override
    public String toString() {
        return name + (isFinalState ? " (Final)" : "");
    }

    /**
     * Compares the specified object with this node for equality. Two nodes are considered equal if they have the same name.
     *
     * @param o The object to be compared with this node.
     * @return True if the given object is equal to this node, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        return name != null ? name.equals(node.name) : node.name == null;
    }

    /**
     * Returns the hash code value for the node. The hash code is based on the node's name.
     *
     * @return The hash code value for this node.
     */
    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
