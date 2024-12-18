package org.piestream.engine;

import org.piestream.events.PointEvent;
import org.piestream.merger.TreeNode;
import org.piestream.parser.MPIEPairSource;
import org.piestream.piepair.eba.EBA;
import org.piestream.merger.BinTree;

import java.util.List;
import java.util.Map;

public class Worker {

    private final MPIEPairsManager mpiEPairsManager;  // MPIEPairs manager
    private final List<MPIEPair> MPPS;  // List of all PIEPair objects
    private final Map<MPIEPairSource, TreeNode> source2Node ;  // Mapping from source to node
    private final BinTree tree;
    private final Window window;

    /**
     * Constructor that creates a tree and maps sources to nodes.
     *
     * @param MPPSourceList List of MPIEPairSource objects
     * @param window        The window used for processing
     * @param EBA2String    Map from EBA objects to strings for representation
     */
    public Worker(List<MPIEPairSource> MPPSourceList, Window window, Map<EBA, String> EBA2String) {

        this.window = window;

        this.tree = new BinTree(MPPSourceList, window, EBA2String);

        try {
            tree.constructTree();
//            System.err.println("Tree constructed successfully.");
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to construct tree");
        }

        // Initialize MPIEPairsManager and retrieve all PIEPair objects
//        this.source2Col = tree.getSource2Col();
        this.source2Node = tree.getSourceToNode();
        this.mpiEPairsManager = new MPIEPairsManager(MPPSourceList, source2Node);
        this.MPPS = mpiEPairsManager.getMPIEPairList();
    }

    /**
     * Gets the BinTree instance.
     *
     * @return BinTree object
     */
    public BinTree getTree() {
        return tree;
    }

    /**
     * Gets the MPIEPairsManager instance.
     *
     * @return MPIEPairsManager object
     */
    public MPIEPairsManager getMpiEPairsManager() {
        return mpiEPairsManager;
    }

    /**
     * Gets the result count from the tree.
     *
     * @return The result count
     */
    public long getResultCNT() {
        return tree.getResultCNT();
    }

    /**
     * Resets the state before running the processing.
     *
     * Clears previous data and refreshes data based on the window type.
     *
     * @param currentTime Current timestamp used to calculate old data
     */
    public void resetBeforeRun(long currentTime) {

        tree.clearMergedNodeData_NewT();
        tree.clearLeafNodeData_NewT();

        if (window.getWindowType() == WindowType.TIME_WINDOW) {
            long deadLine = currentTime - window.getWindowCapacity();
            tree.refreshMergedNodeData_OldT(deadLine);
            tree.refreshLeafNodeData_OldT(deadLine);
        }
    }

    /**
     * Merges the tree data after the run.
     */
    public void mergeAfterRun() {
//        tree.mergeTree();
        tree.mergeTree();
    }

    /**
     * Derives the "before-after" relationships in the tree.
     */
    public void deriveBeforeAfterRel() {
        tree.deriveBeforeAfterRel();
    }

    /**
     * Updates the tree data after the run.
     */
    public void updateData() {

        tree.updateMergedNodeData();
        tree.updateLeafNodeData();
    }

    /**
     * Processes a PointEvent by executing the stepByPE method for each PIEPair.
     *
     * @param pe The PointEvent to process
     */
    public void runOneByOne(PointEvent pe) {
        MPPS.forEach(pair -> pair.run(pe));
    }

}
