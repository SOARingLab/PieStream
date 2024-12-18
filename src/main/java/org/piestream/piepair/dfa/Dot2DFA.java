package org.piestream.piepair.dfa;

import org.piestream.piepair.TemporalRelations;
import org.jgrapht.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * This class provides functionality for parsing DOT files representing Deterministic Finite Automata (DFA),
 * and converting them into DFA objects for use in temporal pattern matching applications.
 * It supports loading a DOT file, extracting states, transitions, and labels, and constructing a DFA model
 * that can be used for pattern recognition with temporal relations.
 */
public class Dot2DFA {

    // Logger to log errors and informational messages
    private static final Logger logger = LoggerFactory.getLogger(Dot2DFA.class);

    /**
     * Parses a DOT file to construct a DFA object.
     *
     * @param dotFilePath the file path of the DOT file.
     * @return a DFA object representing the parsed DOT file, or null if an error occurred.
     */
    public static DFA parseDotFile(String dotFilePath) {
        DFA dfa = new DFA();
        File dotFile = new File(dotFilePath);
        String relationName = dotFile.getName().replace(".dot", "").toUpperCase();

        try {
            // Attempt to parse the relation name from the DOT file name
            TemporalRelations.PreciseRel relation = TemporalRelations.PreciseRel.valueOf(relationName);
            dfa.setRelation(relation);
        } catch (IllegalArgumentException e) {
            // Handle invalid relation name in the DOT file name
            throw new IllegalArgumentException("Invalid relation name in the DOT file name.");
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(dotFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();

                // Skip lines that are part of DOT syntax but don't represent transitions or states
                if (line.startsWith("digraph") || line.startsWith("{") || line.startsWith("}")) {
                    continue;
                }

                // Parse transition lines (fromNode -> toNode [label="transition"])
                if (line.contains("->")) {
                    String[] parts = line.split("->");
                    String fromNodeName = parts[0].trim();
                    String toNodeName = parts[1].split("\\[")[0].trim();
                    String trans = parts[1].contains("label=")
                            ? parts[1].split("label=")[1].split("]")[0].replaceAll("\"", "").trim()
                            : "";

                    // Normalize transition values (if necessary)
                    if (!trans.equals("O") && !trans.equals("I") && !trans.equals("Z") && !trans.equals("E")) {
                        trans = "X";
                    }

                    // Create or retrieve nodes
                    Node fromNode = dfa.getNodeMap().getOrDefault(fromNodeName, new Node(fromNodeName, false));
                    Node toNode = dfa.getNodeMap().getOrDefault(toNodeName, new Node(toNodeName, false));

                    // Add nodes and edge to the DFA graph
                    dfa.getGraph().addVertex(fromNode);
                    dfa.getGraph().addVertex(toNode);
                    dfa.getGraph().addEdge(fromNode, toNode, new LabeledEdge(trans));

                    // Update node map
                    dfa.getNodeMap().put(fromNodeName, fromNode);
                    dfa.getNodeMap().put(toNodeName, toNode);
                }
                // Parse node definition lines (node [shape=doublecircle, initial])
                else if (line.contains("[") && !line.contains("->")) {
                    String nodeName = line.split("\\[")[0].trim();
                    boolean isFinalState = line.contains("shape=doublecircle");
                    Node node = dfa.getNodeMap().getOrDefault(nodeName, new Node(nodeName, isFinalState));

                    if (isFinalState) {
                        node.setFinalState(true);
                    }

                    dfa.getNodeMap().put(nodeName, node);
                    dfa.getGraph().addVertex(node);

                    // If the node is marked as initial, set it as the starting state
                    if (line.contains("initial")) {
                        dfa.setInitState(node);
                        dfa.setCurrentState(node);
                        dfa.setLastState(node);
                    }
                }
            }

            // Ensure that an initial state was found
            if (dfa.getInitState() == null) {
                throw new IllegalArgumentException("Initial state not found in the DOT file.");
            }

            // Convert the graph to a transition map
            dfa.setTransMap(graph2TransMap(dfa.getGraph()));

            return dfa;

        } catch (IOException e) {
            // Log and return null in case of errors during file reading or parsing
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Converts the DOT file's graph into a transition map used by the DFA.
     *
     * @param graph the graph representing the DFA.
     * @return a map of nodes to their corresponding transitions and target nodes.
     */
    public static Map<Node, Map<Alphabet, Node>> graph2TransMap(Graph<Node,LabeledEdge> graph) {
        Map<Node, Map<String, Node>> tempTransitionMap = new HashMap<>();

        // Populate the transition map with the edges from the graph
        graph.edgeSet().forEach(edge -> {
            Node sourceNode = graph.getEdgeSource(edge);
            Node targetNode = graph.getEdgeTarget(edge);
            String trans = ((LabeledEdge) edge).getTrans();

            // If the source node doesn't have a transition map, create one
            tempTransitionMap.computeIfAbsent(sourceNode, k -> new HashMap<>());

            // Add the transition and target node to the source node's map
            tempTransitionMap.get(sourceNode).put(trans, targetNode);
        });

        // Define all possible alphabet values
        Set<String> allAlphabetSet = new HashSet<>(Arrays.asList("O", "I", "Z", "E"));

        Map<Node, Map<Alphabet, Node>> transitionMap = new HashMap<>();

        // Map the transition labels to the enum Alphabet values and handle missing transitions
        for (Map.Entry<Node, Map<String, Node>> entry : tempTransitionMap.entrySet()) {
            Node sourceNode = entry.getKey();
            Map<String, Node> trans2NodeMap = entry.getValue();
            Set<String> sameSet = new HashSet<>(allAlphabetSet);
            sameSet.retainAll(trans2NodeMap.keySet());
            Set<String> differenceSet = new HashSet<>(allAlphabetSet);
            differenceSet.removeAll(trans2NodeMap.keySet());

            for (String sKey : sameSet) {
                transitionMap.computeIfAbsent(sourceNode, k -> new HashMap<>());
                transitionMap.get(sourceNode).put(Alphabet.fromString(sKey), trans2NodeMap.get(sKey));
            }
            for (String dKey : differenceSet) {
                transitionMap.computeIfAbsent(sourceNode, k -> new HashMap<>());
                transitionMap.get(sourceNode).put(Alphabet.fromString(dKey), trans2NodeMap.get("X"));
            }
        }

        return transitionMap;
    }

    /**
     * Creates a DFA from a specified temporal relation.
     *
     * @param relation the temporal relation used to determine the corresponding DOT file.
     * @return a DFA object representing the specified temporal relation.
     */
    public static DFA createDFAFromRelation(TemporalRelations.PreciseRel relation) {
        String resourcePath = Dot2DFA.class.getClassLoader().getResource("dotFiles/").getPath();

        if (resourcePath == null) {
            logger.error("PIE_PAIR_HOME environment variable is not set.");
            System.exit(1);
        }

        String dotFilePath = resourcePath + relation.name() + ".dot";
        return parseDotFile(dotFilePath);
    }


}
