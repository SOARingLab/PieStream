package org.piestream.piepair.dfa;

import org.piestream.piepair.TemporalRelations;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Represents a Deterministic Finite Automaton (DFA) used for processing temporal relations.
 * The DFA transitions between states based on input alphabets and evaluates specific temporal relations.
 * The DFA maintains a directed graph of states and edges where the edges are labeled with transition values.
 */
public class DFA {

    private static final Logger logger = LoggerFactory.getLogger(DFA.class); // Logger for logging DFA operations
    private Graph<Node, LabeledEdge> graph; // Graph representing the DFA with nodes and labeled edges
    private Map<String, Node> nodeMap; // Map of node identifiers to Node objects
    private Node currentState; // The current state of the DFA
    private Node lastState; // The previous state of the DFA
    private Node initState; // The initial state of the DFA
    private TemporalRelations.PreciseRel relation; // The temporal relation associated with the DFA
    private boolean isStateChanged; // Flag to check if the state has changed
    private Alphabet lastAlphabet; // The last alphabet processed by the DFA
    private Alphabet currentAlphabet; // The current alphabet being processed
    private Map<Node, Map<Alphabet, Node>> transMap; // Map of transitions, keyed by node and alphabet

    /**
     * Constructs a DFA without an initial temporal relation.
     */
    public DFA() {
        this.graph = new DefaultDirectedGraph<>(LabeledEdge.class);
        this.nodeMap = new HashMap<>();
        this.isStateChanged = false;
        this.lastAlphabet = null;
        this.currentAlphabet = null;
    }

    /**
     * Constructs a DFA with an initial temporal relation.
     * This initializes the DFA using the specified temporal relation, creating the DFA's graph and state transitions.
     *
     * @param relation The temporal relation for the DFA (e.g., "FINISHES", "EQUALS").
     */
    public DFA(TemporalRelations.PreciseRel relation) {
        this();
        this.relation = relation;
        DFA dfa = Dot2DFA.createDFAFromRelation(relation); // Create DFA from relation
        this.graph = dfa.getGraph();
        this.nodeMap = dfa.getNodeMap();
        this.currentState = dfa.getCurrentState();
        this.lastState = dfa.getLastState();
        this.initState = dfa.getInitState();
    }

    // Getter and Setter methods for various DFA properties

    public void setTransMap(Map<Node, Map<Alphabet, Node>> transMap) {
        this.transMap = transMap; // Set the transition map for state transitions
    }

    public Graph<Node, LabeledEdge> getGraph() {
        return graph; // Return the DFA's graph
    }

    public Map<String, Node> getNodeMap() {
        return nodeMap; // Return the map of nodes
    }

    public Node getCurrentState() {
        return currentState; // Return the current state
    }

    public void setCurrentState(Node currentState) {
        this.currentState = currentState; // Set the current state
    }

    public Node getLastState() {
        return lastState; // Return the last state
    }

    public void setLastState(Node lastState) {
        this.lastState = lastState; // Set the last state
    }

    public Node getInitState() {
        return initState; // Return the initial state
    }

    public void setInitState(Node initState) {
        this.initState = initState; // Set the initial state
    }

    public TemporalRelations.PreciseRel getRelation() {
        return relation; // Return the temporal relation
    }

    public void setRelation(TemporalRelations.PreciseRel relation) {
        this.relation = relation; // Set the temporal relation
    }

    public boolean isStateChanged() {
        return isStateChanged; // Check if the state has changed
    }

    public Alphabet getLastAlphabet() {
        return lastAlphabet; // Return the last alphabet
    }

    public Alphabet getCurrentAlphabet() {
        return currentAlphabet; // Return the current alphabet
    }

    /**
     * Prints the current graph of the DFA, showing its vertices and edges.
     * For debugging purposes, this method logs the vertices and edges in the graph.
     */
    public void printGraph() {
        logger.info("Graph vertices:");
        for (Node vertex : graph.vertexSet()) {
            // Optionally log each vertex
//            logger.info(vertex);
        }

        logger.info("\nGraph edges:");
        for (LabeledEdge edge : graph.edgeSet()) {
            logger.info(graph.getEdgeSource(edge) + " -> " + graph.getEdgeTarget(edge) + " [trans=" + edge.getTrans() + "]");
        }
    }

    /**
     * Checks if the current state is a final state.
     *
     * @return true if the current state is final, false otherwise.
     */
    public boolean isFinalState() {
        return currentState != null && currentState.isFinalState(); // Check if the current state is final
    }

    /**
     * Checks if the DFA is in a triggering state.
     * A trigger occurs when the last state was not final, and the current state is final.
     *
     * @return true if the DFA is in a trigger state, false otherwise.
     */
    public boolean isTrigger() {
        return (lastState != null && !lastState.isFinalState() && currentState != null && currentState.isFinalState());
    }

    /**
     * Determines if the DFA has completed based on the relation.
     * A completion occurs when the DFA transitions from a final state to a non-final state for specific temporal relations.
     *
     * @return true if the DFA has completed, false otherwise.
     */
    public boolean isCompleted() {
        if (relation == TemporalRelations.PreciseRel.FINISHES ||
                relation == TemporalRelations.PreciseRel.FINISHED_BY ||
                relation == TemporalRelations.PreciseRel.EQUALS) {
            return isTrigger();
        } else {
            return (lastState != null && lastState.isFinalState() && currentState != null && !currentState.isFinalState());
        }
    }

    /**
     * Performs a transition in the DFA based on the provided alphabet.
     * This method updates the current state of the DFA based on the transition map and checks if the state has changed.
     *
     * @param alphabet The input alphabet triggering the state transition.
     * @return The new current state after the transition.
     */
    public Node step(Alphabet alphabet) {
        if (currentState == null) {
            return null; // Return null if the current state is not set
        }

        // If current state is initState, first try to transition based on "X" transition
        if (currentState.equals(initState)) {
            Set<LabeledEdge> initOutgoingEdges = graph.outgoingEdgesOf(initState);
            for (LabeledEdge edge : initOutgoingEdges) {
                if (edge.getTrans().equals("X")) {
                    lastState = currentState;
                    currentState = graph.getEdgeTarget(edge);
                    checkAndUpdateStateChange(); // Check and update state change
                }
            }
            // Proceed with normal step operation
            Node nextState = findNextState(currentState, alphabet);
            lastState = currentState;
            currentState = nextState;
            return currentState;

        } else {
            // Proceed with normal step operation
            Node nextState = findNextState(currentState, alphabet);
            lastState = currentState;
            currentState = nextState;
            checkAndUpdateStateChange(); // Check and update state change
            return currentState;
        }
    }

    /**
     * Finds the next state in the DFA based on the current state and the input alphabet.
     *
     * @param currentState The current state of the DFA.
     * @param alphabet The input alphabet triggering the transition.
     * @return The next state after the transition.
     */
    private Node findNextState(Node currentState, Alphabet alphabet) {
        return transMap.get(currentState).get(alphabet); // Find the next state using the transition map
    }

    /**
     * Checks if the state has changed and updates the state change flag.
     */
    private void checkAndUpdateStateChange() {
        isStateChanged = !currentState.equals(lastState); // Update the state change flag
    }
}
