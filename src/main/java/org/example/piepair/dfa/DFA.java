package org.example.piepair.dfa;

import org.example.piepair.TemporalRelations;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DFA {

    private Graph<Node, LabeledEdge> graph;
    private Map<String, Node> nodeMap;
    private Node currentState;
    private Node lastState;
    private Node initState;
    private TemporalRelations.PreciseRel relation;
    private boolean isStateChanged;
    private Alphabet lastAlphabet;
    private Alphabet currentAlphabet;

    public DFA() {
        this.graph = new DefaultDirectedGraph<>(LabeledEdge.class);
        this.nodeMap = new HashMap<>();
        this.isStateChanged = false;
        this.lastAlphabet = null;
        this.currentAlphabet = null;
    }

    public DFA(TemporalRelations.PreciseRel relation) {
        this();
        this.relation = relation;
        DFA dfa = Dot2DFA.createDFAFromRelation(relation);
        this.graph = dfa.getGraph();
        this.nodeMap = dfa.getNodeMap();
        this.currentState = dfa.getCurrentState();
        this.lastState = dfa.getLastState();
        this.initState = dfa.getInitState();
    }

    public Graph<Node, LabeledEdge> getGraph() {
        return graph;
    }

    public Map<String, Node> getNodeMap() {
        return nodeMap;
    }

    public Node getCurrentState() {
        return currentState;
    }

    public void setCurrentState(Node currentState) {
        this.currentState = currentState;
    }

    public Node getLastState() {
        return lastState;
    }

    public void setLastState(Node lastState) {
        this.lastState = lastState;
    }

    public Node getInitState() {
        return initState;
    }

    public void setInitState(Node initState) {
        this.initState = initState;
    }

    public TemporalRelations.PreciseRel getRelation() {
        return relation;
    }

    public void setRelation(TemporalRelations.PreciseRel relation) {
        this.relation = relation;
    }

    public boolean isStateChanged() {
        return isStateChanged;
    }

    public Alphabet getLastAlphabet() {
        return lastAlphabet;
    }

    public Alphabet getCurrentAlphabet() {
        return currentAlphabet;
    }

    public void printGraph() {
        System.out.println("Graph vertices:");
        for (Node vertex : graph.vertexSet()) {
            System.out.println(vertex);
        }

        System.out.println("\nGraph edges:");
        for (LabeledEdge edge : graph.edgeSet()) {
            System.out.println(graph.getEdgeSource(edge) + " -> " + graph.getEdgeTarget(edge) + " [trans=" + edge.getTrans() + "]");
        }
    }

    public boolean isFinalState() {
        return currentState != null && currentState.isFinalState();
    }

    public boolean isTrigger() {
        return (lastState != null && !lastState.isFinalState() && currentState != null && currentState.isFinalState());
    }

    public boolean isCompleted() {
        if (relation == TemporalRelations.PreciseRel.FINISHES ||
                relation == TemporalRelations.PreciseRel.FINISHED_BY ||
                relation == TemporalRelations.PreciseRel.EQUALS) {
            return isTrigger();
        } else {
            return (lastState != null && lastState.isFinalState() && currentState != null && !currentState.isFinalState());
        }
    }

    public Node step(Alphabet alphabet) {
        if (currentState == null) {
            return null;
        }

        lastAlphabet = currentAlphabet;
        currentAlphabet = alphabet;

        // If current state is initState, first try to transition based on trans="X"
        if (currentState.equals(initState)) {
            Set<LabeledEdge> initOutgoingEdges = graph.outgoingEdgesOf(initState);
            for (LabeledEdge edge : initOutgoingEdges) {
                if (edge.getTrans().equals("X")) {
                    lastState = currentState;
                    currentState = graph.getEdgeTarget(edge);
                    checkAndUpdateStateChange();
                    return currentState;
                }
            }
        }

        // Proceed with normal step operation
        Node nextState = findNextState(currentState, alphabet);
        lastState = currentState;
        currentState = nextState;
        checkAndUpdateStateChange();
        return currentState;
    }

    private Node findNextState(Node currentState, Alphabet alphabet) {
        Set<LabeledEdge> outgoingEdges = graph.outgoingEdgesOf(currentState);
        for (LabeledEdge edge : outgoingEdges) {
            if (edge.getTrans().equals(alphabet.toString())) {
                return graph.getEdgeTarget(edge);
            }
        }

        for (LabeledEdge edge : outgoingEdges) {
            if (edge.getTrans().equals("X")) {
                return graph.getEdgeTarget(edge);
            }
        }

        // If no valid transition is found, throw an exception or log an error
        throw new IllegalArgumentException("No valid transition found for input: " + alphabet);
    }

    private void checkAndUpdateStateChange() {
        isStateChanged = !currentState.equals(lastState);
    }
}
