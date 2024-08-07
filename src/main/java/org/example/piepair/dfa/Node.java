package org.example.piepair.dfa;

import java.io.Serializable;

public class Node implements Serializable {
    private String name;
    private boolean isFinalState;

    public Node(String name, boolean isFinalState) {
        this.name = name;
        this.isFinalState = isFinalState;
    }

    public String getName() {
        return name;
    }

    public boolean isFinalState() {
        return isFinalState;
    }

    public void setFinalState(boolean finalState) {
        isFinalState = finalState;
    }

    @Override
    public String toString() {
        return name + (isFinalState ? " (Final)" : "");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        return name != null ? name.equals(node.name) : node.name == null;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
