package org.example.piepair.dfa;

import org.example.piepair.TemporalRelations;
import org.jgrapht.Graph;

import java.io.*;
import java.util.*;

public class Dot2DFA {

    public static DFA parseDotFile(String dotFilePath) {
        DFA dfa = new DFA();
        File dotFile = new File(dotFilePath);
        String relationName = dotFile.getName().replace(".dot", "").toUpperCase();

        try {
            TemporalRelations.PreciseRel relation = TemporalRelations.PreciseRel.valueOf(relationName);
            dfa.setRelation(relation);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid relation name in the DOT file name.");
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(dotFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("digraph") || line.startsWith("{") || line.startsWith("}")) {
                    continue;
                }
                if (line.contains("->")) {
                    String[] parts = line.split("->");
                    String fromNodeName = parts[0].trim();
                    String toNodeName = parts[1].split("\\[")[0].trim();
                    String trans = parts[1].contains("label=") ? parts[1].split("label=")[1].split("]")[0].replaceAll("\"", "").trim() : "";

                    // 如果trans值不是"O", "I", "Z", "E"，则改为"X"
                    if (!trans.equals("O") && !trans.equals("I") && !trans.equals("Z") && !trans.equals("E")) {
                        trans = "X";
                    }

                    Node fromNode = dfa.getNodeMap().getOrDefault(fromNodeName, new Node(fromNodeName, false));
                    Node toNode = dfa.getNodeMap().getOrDefault(toNodeName, new Node(toNodeName, false));

                    dfa.getGraph().addVertex(fromNode);
                    dfa.getGraph().addVertex(toNode);
                    dfa.getGraph().addEdge(fromNode, toNode, new LabeledEdge(trans));

                    dfa.getNodeMap().put(fromNodeName, fromNode);
                    dfa.getNodeMap().put(toNodeName, toNode);
                } else if (line.contains("[") && !line.contains("->")) {
                    String nodeName = line.split("\\[")[0].trim();
                    boolean isFinalState = line.contains("shape=doublecircle");
                    Node node = dfa.getNodeMap().getOrDefault(nodeName, new Node(nodeName, isFinalState));
                    if (isFinalState) {
                        node.setFinalState(true);
                    }
                    dfa.getNodeMap().put(nodeName, node);
                    dfa.getGraph().addVertex(node);

                    if (line.contains("initial")) {
                        dfa.setInitState(node);
                        dfa.setCurrentState(node);
                        dfa.setLastState(node);
                    }
                }
            }

            if (dfa.getInitState() == null) {
                throw new IllegalArgumentException("Initial state not found in the DOT file.");
            }
            dfa.setTransMap(graph2TransMap(dfa.getGraph()));

            return dfa;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Map<Node, Map<Alphabet, Node>> graph2TransMap(Graph<Node,LabeledEdge> graph) {
        Map<Node, Map<String, Node>> tempTransitionMap = new HashMap<>();

        graph.edgeSet().forEach(edge -> {
            Node sourceNode = graph.getEdgeSource(edge);
            Node targetNode = graph.getEdgeTarget(edge);
            String trans = ((LabeledEdge) edge).getTrans() ;  // Get the label from LabeledEdge

            // If the source node does not already have a transition map, create one
            tempTransitionMap.computeIfAbsent(sourceNode, k -> new HashMap<>());

            // Add the transition and target node to the source node's transition map
            tempTransitionMap.get(sourceNode).put(trans, targetNode);
        });
        Set<String> allAlphabetSet = new HashSet<>(Arrays.asList("O", "I", "Z", "E"));

        Map<Node, Map<Alphabet, Node>> transitionMap = new HashMap<>();

        for( Map.Entry<Node, Map<String,Node>> entry   : tempTransitionMap.entrySet()){
            Node sourceNode=entry.getKey();
            Map<String, Node> trans2NodeMap= entry.getValue();
            Set<String> sameSet = new HashSet<>(allAlphabetSet);
            sameSet.retainAll(trans2NodeMap.keySet());
            Set<String> differenceSet = new HashSet<>(allAlphabetSet);
            differenceSet.removeAll(trans2NodeMap.keySet());
            for(String sKey : sameSet){
                transitionMap.computeIfAbsent(sourceNode, k -> new HashMap<>());
                transitionMap.get(sourceNode).put( Alphabet.fromString(sKey),trans2NodeMap.get(sKey)  );
            }
            for(String dKey : differenceSet){
                transitionMap.computeIfAbsent(sourceNode, k -> new HashMap<>());
                transitionMap.get(sourceNode).put( Alphabet.fromString(dKey),trans2NodeMap.get("X")  );
            }

        }

        return transitionMap;
    }


    public static DFA createDFAFromRelation(TemporalRelations.PreciseRel relation) {
        String dotFilePath = "src/main/resources/dotFiles/" + relation.name() + ".dot";
        return parseDotFile(dotFilePath);
    }

    public static void main(String[] args) {
        String dotFilePath = "src/main/resources/dotFiles/FINISHED_BY.dot";

        DFA dfa = parseDotFile(dotFilePath);

        if (dfa != null) {
            dfa.printGraph();

            // 使用 dfa 进行其他操作
        } else {
            System.out.println("Failed to parse DOT file.");
        }
    }
}
