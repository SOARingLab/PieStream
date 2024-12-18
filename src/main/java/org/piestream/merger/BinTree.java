package org.piestream.merger;

import org.piestream.engine.Window;
import org.piestream.parser.MPIEPairSource;
import org.piestream.piepair.IEP;
import org.piestream.piepair.eba.EBA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.TreeSet;
import java.util.Set;

/**
 * BinTree class builds and manages a binary tree structure for merging multiple data sources based on IEP predicates.
 * It constructs a binary tree, manages merged nodes, and performs various operations related to the tree structure.
 */
public class BinTree {
    private static final Logger logger = LoggerFactory.getLogger(BinTree.class);
    private final List<MPIEPairSource> sourceList;
    private final Map<MPIEPairSource, IEPCol> source2Col;
    private final Map<MPIEPairSource, TreeNode> sourceToNode;
    private final Map<IEPCol, TreeNode> Col2Node;
    private TreeNode root;
    private TreeNode bottomLeaf;
    private final List<TreeNode> mergedNodes;
    private final Map<EBA, String> EBA2String;
    private final Window window;
    private final Map<TreeNode, List<String>> node2JoinedCols;

    private Merger merger = null;
    private final Map<TreeNode, Set<String>> node2AcmltJoinedCols;

    /**
     * Constructs a BinTree object with the given sources, window, and EBA to String mapping.
     *
     * @param sourceList The list of sources to be merged in the binary tree
     * @param window The window associated with the binary tree
     * @param EBA2String A map from EBA (Event-Based Actions) to their string representations
     */
    public BinTree(List<MPIEPairSource> sourceList, Window window, Map<EBA, String> EBA2String) {
        this.sourceList = sourceList;
        this.window = window;
        this.source2Col = new HashMap<>();
        this.sourceToNode = new HashMap<>();
        this.Col2Node = new HashMap<>();
        this.root = null;
        this.bottomLeaf = null;
        this.EBA2String = EBA2String;
        this.mergedNodes = new ArrayList<>();
        this.node2JoinedCols = new HashMap<>();
        this.node2AcmltJoinedCols = new HashMap<>();
    }

    /**
     * Constructs a binary tree from the list of sources.
     * The method will create leaf nodes and merge them into a tree structure.
     *
     * @return The root of the constructed binary tree
     * @throws Exception if no nodes can be merged or an error occurs during the construction
     */
    public TreeNode constructTree() throws Exception {
        List<TreeNode> nodes = createLeafNodes(); // Create leaf nodes from sources

        // Throw an exception if no nodes are available for merging
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("No nodes to merge");
        }

        root = createTree(nodes); // Merge the nodes into a binary tree

        // After the tree is constructed, build the "joinedCols" for each Table
        this.node2JoinedCols.put(root, null);
        this.node2AcmltJoinedCols.put(root, null);
        buildJoinedCols(root);

        this.merger = new Merger(root, bottomLeaf, EBA2String, node2JoinedCols);
        return root;
    }

    /**
     * Recursively builds the joined columns for the binary tree nodes.
     *
     * @param node The node to start building the joined columns from
     */
    private void buildJoinedCols(TreeNode node) {
        if (node != null && !node.isLeaf) {
            List<String> joinedCols = new ArrayList<>();
            for (EBA eba : node.getKeyPredSet()) {
                joinedCols.add(EBA2String.get(eba)); // Add string representation of EBA
            }
            TreeNode left = node.getLeft();
            TreeNode right = node.getRight();
            Set<String> acmlt = new TreeSet<>(joinedCols);
            if (node2AcmltJoinedCols.get(node) != null) {
                acmlt.addAll(node2AcmltJoinedCols.get(node));
            }
            if (left != null) {
                this.node2AcmltJoinedCols.putIfAbsent(left, new TreeSet<>());
                CollectAcmltWithTableCols(left, new TreeSet<>(acmlt));
                // Update node2JoinedCols, overriding the old value
                this.node2JoinedCols.put(left, new ArrayList<>(joinedCols));
            }
            if (right != null) {
                this.node2AcmltJoinedCols.putIfAbsent(right, new TreeSet<>());
                CollectAcmltWithTableCols(right, new TreeSet<>(acmlt));
                // Update node2JoinedCols, overriding the old value
                this.node2JoinedCols.put(right, new ArrayList<>(joinedCols));
            }
            buildJoinedCols(left); // Recursively build joined columns for left child
            buildJoinedCols(right); // Recursively build joined columns for right child
        }
    }

    /**
     * Collects and updates the accumulated joined columns for a node.
     *
     * @param node The node to update
     * @param acmlt The accumulated columns to be updated
     */
    private void CollectAcmltWithTableCols(TreeNode node, Set<String> acmlt) {
        Set<String> predSets = new TreeSet<>();
        for (EBA eba : node.getPredSet()) {
            predSets.add(EBA2String.get(eba)); // Add string representation of EBA
        }
        acmlt.retainAll(predSets); // Retain only those in common with predSets

        this.node2AcmltJoinedCols.put(node, acmlt); // Update the accumulated joined columns for the node
    }

    /**
     * Creates leaf nodes from the provided sources.
     * Each source is associated with an IEPCol and a TreeNode.
     *
     * @return A list of leaf nodes
     */
    private List<TreeNode> createLeafNodes() {
        List<TreeNode> nodes = new ArrayList<>();
        for (MPIEPairSource source : sourceList) {
            Set<EBA> predSet = new HashSet<>();
            predSet.add(source.getFormerPred()); // Add the former predicate
            predSet.add(source.getLatterPred()); // Add the latter predicate
            // Create a new IEPCol for this source
            IEPCol Col = new IEPCol(window, EBA2String);
            TreeNode node = new TreeNode(predSet, source, window, EBA2String);

            source2Col.put(source, Col);
            Col2Node.put(Col, node);
            sourceToNode.put(source, node); // Map source to node
            nodes.add(node);
        }
        return nodes;
    }

    /**
     * Merges a list of nodes into a binary tree structure.
     *
     * @param nodes A list of nodes to be merged
     * @return The root node of the merged binary tree
     * @throws Exception if the tree cannot be constructed
     */
    private TreeNode createTree(List<TreeNode> nodes) throws Exception {
        int heightCnt = 0; // Initialize height counter
        TreeNode hNode = nodes.remove(0); // Take the first node as the starting point
        bottomLeaf = hNode; // Set the bottom leaf to the current node
        hNode.height = heightCnt;
        heightCnt++;
        hNode = createParentNode(hNode, null, heightCnt); // Create the parent node by merging hNode and null
        mergedNodes.add(hNode);

        while (!nodes.isEmpty()) {
            boolean mergedCurrent = false;
            for (int i = 0; i < nodes.size(); i++) {
                TreeNode otherNode = nodes.get(i);
                if (canMerge(hNode, otherNode)) { // Check if hNode and otherNode can be merged
                    if (heightCnt == 0) {
                        bottomLeaf = hNode;
                    }
                    TreeNode parentNode = createParentNode(hNode, otherNode, heightCnt); // Merge the nodes
                    heightCnt++;
                    hNode = parentNode; // Update hNode to the new merged parent node
                    mergedNodes.add(hNode);
                    nodes.remove(i); // Remove the merged node from the list
                    mergedCurrent = true;
                    break;
                }
            }

            if (!mergedCurrent) {
                throw new Exception("Failed to construct binary tree");
            }
        }

        return hNode; // Return the root node
    }

    /**
     * Determines if two nodes can be merged based on their predicate sets.
     *
     * @param hNode The first node
     * @param otherNode The second node
     * @return true if the nodes can be merged, false otherwise
     */
    private boolean canMerge(TreeNode hNode, TreeNode otherNode) {
        Set<EBA> intersection = new HashSet<>(hNode.predSet);
        intersection.retainAll(otherNode.predSet); // Find the intersection of predicates
        return !intersection.isEmpty(); // Nodes can merge if they share any predicates
    }

    /**
     * Merges two nodes and updates their attributes accordingly.
     *
     * @param hNode The first node
     * @param otherNode The second node
     * @param heightCnt The current height of the tree
     * @return The new parent node created by merging the two nodes
     */
    private TreeNode createParentNode(TreeNode hNode, TreeNode otherNode, int heightCnt) {
        Set<EBA> newPredSet = new HashSet<>(hNode.predSet); // Initialize the new predicate set with hNode's predicates

        if (otherNode != null) {
            newPredSet.addAll(otherNode.predSet); // Merge otherNode's predicates
        }

        Set<EBA> newKeyPredSet = new HashSet<>(hNode.predSet);

        if (otherNode != null) {
            newKeyPredSet.retainAll(otherNode.predSet); // Find the intersection of predicates
        }

        hNode.height = heightCnt + 1;

        if (otherNode != null) {
            otherNode.height = heightCnt + 1;
            otherNode.brother = hNode;
            hNode.brother = otherNode;
        }

        TreeNode mergedNode = new TreeNode(newPredSet, newKeyPredSet, hNode, otherNode, heightCnt + 1, window, EBA2String);

        hNode.parent = mergedNode;
        if (otherNode != null) {
            otherNode.parent = mergedNode;
        }
        return mergedNode;
    }
    /**
     * Merges the tree by invoking the mergeTree method of the Merger class.
     */
    public void mergeTree() {
        merger.mergeTree();
    }

    /**
     * Derives the "before" and "after" relationships for each leaf node.
     * The bottom leaf derives all previous IEPs, while other leaf nodes only derive key IEPs.
     * The remaining IEPs are derived during the merge stage to optimize computation.
     */
    public void deriveBeforeAfterRel() {
        for (Map.Entry<IEPCol, TreeNode> entry : Col2Node.entrySet()) {
            TreeNode node = entry.getValue();
            node.deriveBeforeAfterRel(node == bottomLeaf); // Derive relationships only for leaf nodes
        }
    }

    /**
     * Retrieves the result count from the root node.
     *
     * @return The result count of the root node
     */
    public long getResultCNT() {
        return root.getResCount();
    }

    /**
     * Updates the data for merged nodes by concatenating their new data and adjusting their result counts.
     */
    public void updateMergedNodeData() {
        for (TreeNode node : mergedNodes) {
            if (node == root) {
                long time = node.newT.addDetectTimeAndCalProcessTime("detectTime", System.nanoTime());
                node.addProcessTime(time);
                node.T.concatenate(node.newT); // Merge new data into the node's table
                node.addResCount(node.newT.getSize()); // Update result count
            } else {
                node.T.concatenate(node.newT); // Merge new data for non-root nodes
                node.addResCount(node.newT.getSize());
            }
        }
    }

    /**
     * Updates the data for all leaf nodes, including the columns related to "before" and "after" predicates.
     */
    public void updateLeafNodeData() {
        for (Map.Entry<IEPCol, TreeNode> entry : Col2Node.entrySet()) {
            TreeNode node = entry.getValue();
            node.getCol().updateIEP2List(); // Update the IEP list for the node's column
            if (node.isHasBefore()) {
                node.getBefCol().updateIEP2List(); // Update the "before" column if present
            }
            if (node.isHasAfter()) {
                node.getAftCol().updateIEP2List(); // Update the "after" column if present
            }
        }
    }

    /**
     * Clears the new table data for all leaf nodes and resets their trigger states.
     */
    public void clearLeafNodeData_NewT() {
        for (Map.Entry<IEPCol, TreeNode> entry : Col2Node.entrySet()) {
            TreeNode leafNode = entry.getValue();
            leafNode.getCol().resetIsTrigger(); // Reset the trigger state for the column
            if (leafNode.isHasBefore()) {
                leafNode.getBefCol().resetIsTrigger(); // Reset the trigger for the "before" column
            }
            if (leafNode.isHasAfter()) {
                leafNode.getAftCol().resetIsTrigger(); // Reset the trigger for the "after" column
            }
        }
    }

    /**
     * Clears the new table data for all merged nodes.
     */
    public void clearMergedNodeData_NewT() {
        for (TreeNode node : mergedNodes) {
            Table newT = node.newT;
            if (newT.getSize() != 0) {
                newT.clear(); // Clear the new table data if it has any content
            }
        }
    }

    /**
     * Refreshes the data of merged nodes based on the provided deadline.
     * This ensures the data is up to date according to the time constraints.
     *
     * @param deadLine The deadline to refresh the data
     */
    public void refreshMergedNodeData_OldT(long deadLine) {
        for (TreeNode node : mergedNodes) {
            Table oldT = node.T;
            if (oldT.getSize() != 0) {
                oldT.refresh(deadLine); // Refresh the old table data based on the deadline
            }
        }
    }

    /**
     * Refreshes the data of all leaf nodes based on the provided deadline.
     * This also clears expired IEPs from the columns of the leaf nodes.
     *
     * @param deadLine The deadline to refresh the data
     */
    public void refreshLeafNodeData_OldT(long deadLine) {
        for (Map.Entry<IEPCol, TreeNode> entry : Col2Node.entrySet()) {
            TreeNode leafNode = entry.getValue();
            List<IEP> toDelIeps = leafNode.getCol().refresh(deadLine); // Refresh the column data
            if (leafNode.isHasBefore()) {
                leafNode.formerIEList.refresh(deadLine); // Refresh the "before" IEP list
                toDelIeps = leafNode.getBefCol().refresh(deadLine); // Refresh the "before" column
            }
            if (leafNode.isHasAfter()) {
                leafNode.latterIEList.refresh(deadLine); // Refresh the "after" IEP list
                toDelIeps = leafNode.getAftCol().refresh(deadLine); // Refresh the "after" column
            }
        }
    }

    /**
     * Updates the root table with the provided list of deleted IEPs by adjusting their event times.
     *
     * @param toDelIeps The list of IEPs to be updated in the root table
     */
    private void updateIepET2RootTable(List<IEP> toDelIeps) {
        Table rootTable = root.getT();
        for (IEP iep : toDelIeps) {
            if (iep.getCompTime() == IEP.CompletedTime.LatterEnd) {
                rootTable.update(EBA2String.get(iep.getLatterPie()), iep.getLatterStartTime(), iep.getLatterEndTime()); // Update the table with "latter" IEP times
            } else if (iep.getCompTime() == IEP.CompletedTime.FormerEnd) {
                rootTable.update(EBA2String.get(iep.getFormerPie()), iep.getFormerStartTime(), iep.getFormerEndTime()); // Update the table with "former" IEP times
            } else {
                return; // No update required for IEPs without a completion time
            }
        }
    }

    // Getter methods

    /**
     * Returns the list of source data.
     *
     * @return The source list
     */
    public List<MPIEPairSource> getSourceList() {
        return sourceList;
    }

    /**
     * Returns a map from source data to IEP columns.
     *
     * @return The map of source-to-IEP column mappings
     */
    public Map<MPIEPairSource, IEPCol> getSource2Col() {
        return source2Col;
    }

    /**
     * Returns a map from source data to tree nodes.
     *
     * @return The map of source-to-tree node mappings
     */
    public Map<MPIEPairSource, TreeNode> getSourceToNode() {
        return sourceToNode;
    }

    /**
     * Returns a map from IEP columns to tree nodes.
     *
     * @return The map of IEP column-to-tree node mappings
     */
    public Map<IEPCol, TreeNode> getCol2Node() {
        return Col2Node;
    }

    /**
     * Returns the root node of the binary tree.
     *
     * @return The root tree node
     */
    public TreeNode getRoot() {
        return root;
    }

    /**
     * Returns the bottom leaf node of the binary tree.
     *
     * @return The bottom leaf tree node
     */
    public TreeNode getBottomLeaf() {
        return bottomLeaf;
    }

    /**
     * Returns a map from EBAs to their string representations.
     *
     * @return The map of EBA-to-string mappings
     */
    public Map<EBA, String> getEBA2String() {
        return EBA2String;
    }

    /**
     * Returns the Merger instance associated with this binary tree.
     *
     * @return The Merger object
     */
    public Merger getMerger() {
        return merger;
    }
}
