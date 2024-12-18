package org.piestream.merger;

import org.piestream.events.Expirable;
import org.piestream.piepair.IE;
import org.piestream.piepair.eba.EBA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.lang.Math.min;

public class Merger {
    private static final Logger logger = LoggerFactory.getLogger(Merger.class);
    private final TreeNode root;  // Root node of the tree structure
    private final TreeNode bottomLeaf;  // The bottom leaf node where the merging process begins
    private final Map<TreeNode, List<String>> node2JoinedCols;  // Mapping of TreeNode to the columns that are joined
    private final Map<EBA, String> EBA2String;  // Mapping of EBA to a string representation

    public long deriveJoinBefAft = 0;  // Time taken for deriving join results for "before" and "after"
    public long normalJoin = 0;  // Time taken for normal join operations
    public long startTime = 0;  // Start time for measuring the execution duration
    public long endTime = 0;  // End time for measuring the execution duration

    // Constructor to initialize the Merger with the provided parameters
    public Merger(TreeNode root, TreeNode bottomLeaf, Map<EBA, String> EBA2String, Map<TreeNode, List<String>> node2JoinedCols) {
        this.root = root;
        this.bottomLeaf = bottomLeaf;
        this.EBA2String = EBA2String;
        this.node2JoinedCols = node2JoinedCols;
    }

    // Prior to joining, unify the current round's matching results (including before and after) into the NewTable under the leaf node's column
    // This method updates the NewIepTo Col.NewT
    private void refreshNewIepTable_Leaf(TreeNode leaf) {
        // Update the new IEP list to the column's new table for the leaf node
        leaf.getCol().updateNewIepList2Table(EBA2String, node2JoinedCols.get(leaf));

        // If the leaf node has a "before" column, update the new IEP list and merge with the main column
        if (leaf.isHasBefore()) {
            leaf.getBefCol().updateNewIepList2Table(EBA2String, node2JoinedCols.get(leaf));
            leaf.getCol().mergeBefAftCol(leaf.getBefCol());
        }

        // If the leaf node has an "after" column, update the new IEP list and merge with the main column
        if (leaf.isHasAfter()) {
            leaf.getAftCol().updateNewIepList2Table(EBA2String, node2JoinedCols.get(leaf));
            leaf.getCol().mergeBefAftCol(leaf.getAftCol());
        }
    }

    // Update the NewIep list to Col.NewT, BefCol.NewT, and AftCol.NewT
    private void updateNewIepToTable(TreeNode leaf) {
        // Update the new IEP list to the column's new table for the leaf node
        leaf.getCol().updateNewIepList2Table(EBA2String, node2JoinedCols.get(leaf));

        // If the leaf node has a "before" column, update it as well
        if (leaf.isHasBefore()) {
            leaf.getBefCol().updateNewIepList2Table(EBA2String, node2JoinedCols.get(leaf));
        }

        // If the leaf node has an "after" column, update it as well
        if (leaf.isHasAfter()) {
            leaf.getAftCol().updateNewIepList2Table(EBA2String, node2JoinedCols.get(leaf));
        }
    }

    // Merge the bottom leaf node and propagate the changes up the tree
    public void megreBottomLeaf() {
        // Refresh the IEP table for the bottom leaf node
        refreshNewIepTable_Leaf(bottomLeaf);

        TreeNode parentNode = bottomLeaf.parent;

        // If the parent node is the root, directly concatenate the new IEP table of the leaf node
        if (parentNode == root) {
            parentNode.newT.concatenate(bottomLeaf.getCol().getNewIEPTable());  // Directly concatenate -> finish
        } else {
            // Otherwise, concatenate the new IEP table with rebuilding the index
            parentNode.newT.concatenate(bottomLeaf.getCol().getNewIEPTable(),
                    node2JoinedCols.get(parentNode),
                    node2JoinedCols.get(bottomLeaf));  // Rebuild the index during concatenation
        }
    }

    // Perform the merge operation for the entire tree, starting from the bottom leaf
    public void mergeTree() {
        // First, merge the bottom leaf
        megreBottomLeaf();

        // Traverse up the tree to merge intermediate nodes
        TreeNode mergedNode = bottomLeaf.parent;
        TreeNode parentNode = mergedNode.parent;
        TreeNode leafNode = mergedNode.brother;

        while (parentNode != null) {
            // Merge the intermediate node and propagate changes upwards
            mergeIntermedNode(parentNode, mergedNode, leafNode);
            mergedNode = parentNode;
            parentNode = parentNode.parent;
            leafNode = mergedNode.brother;
        }
    }

    // Merge intermediate nodes during the tree merging process.
    // This method processes the intermediate node, updating its tables based on the current leaf node.
    private void mergeIntermedNode(TreeNode ParentNode, TreeNode mergedNode, TreeNode leafNode) {

        startTime = System.currentTimeMillis();  // Record the start time for performance tracking
        updateNewIepToTable(leafNode);  // Update the new IEP table for the leaf node

        // Retrieve the tables from the merged node and leaf node
        Table intermNewTab = mergedNode.getNewT();
        Table intermOldTab = mergedNode.getT();
        Table leafOldTab = leafNode.getCol().getIEPTable();

        // Perform normal join for results not in BefCol.NewT and AftCol.NewT
        // This is a standard incremental natural join operation
        incrementalNaturalJoin(ParentNode, intermNewTab, intermOldTab, leafNode.getCol().getNewIEPTable(), leafOldTab);
        endTime = System.currentTimeMillis();  // Record the end time
        normalJoin += (endTime - startTime);  // Add the duration of this join to the total time

        startTime = System.currentTimeMillis();  // Start a new time measurement for deriving join results

        // Set of key predicates for the current leaf node's parent
        Set<EBA> keyPredSet = leafNode.parent.keyPredSet;
        EBA earlyPie;
        EBA laterPie;

        // If the leaf node has an "after" column, handle merging for the "after" result
        if (leafNode.hasAfter) {
            earlyPie = leafNode.mpp.getLatterPred();  // Get the later predicate for "after" processing
            laterPie = leafNode.mpp.getFormerPred();  // Get the former predicate for "after" processing
            mergeIntermedNodeDerivingAfter(ParentNode, leafNode, keyPredSet, earlyPie, laterPie, intermNewTab, intermOldTab);
        }

        // If the leaf node has a "before" column, handle merging for the "before" result
        if (leafNode.hasBefore) {
            earlyPie = leafNode.mpp.getFormerPred();  // Get the former predicate for "before" processing
            laterPie = leafNode.mpp.getLatterPred();  // Get the later predicate for "before" processing
            mergeIntermedNodeDerivingBefore(ParentNode, leafNode, keyPredSet, earlyPie, laterPie, intermNewTab, intermOldTab);
        }

        endTime = System.currentTimeMillis();  // Record the end time for deriving join
        deriveJoinBefAft += (endTime - startTime);  // Add the duration of the derive join to the total time
    }

    // Handle merging intermediate nodes when deriving the "before" results.
    // This method processes different join cases based on key predicates.
    private void mergeIntermedNodeDerivingBefore(TreeNode ParentNode, TreeNode leafNode, Set<EBA> keyPredSet, EBA earlyPie, EBA laterPie, Table intermNewTab, Table intermOldTab) {

        EBA joinedPie;
        EBA unJoinedPie;

        // 3. Join on both: First join on the latter predicate, then derive the former predicate using the intermTable
        if (keyPredSet.contains(earlyPie) && keyPredSet.contains(laterPie)) {
            increDeriveFromIntermAndIEListJoin(ParentNode, earlyPie, laterPie, intermNewTab, intermOldTab, leafNode.getBefCol().getNewIEPTable(), leafNode.getBefCol().getIEPTable());
        }
        // 1. Join on the former predicate: First traverse the intermTable, then derive the former predicate
        else if (keyPredSet.contains(earlyPie)) {
            joinedPie = earlyPie;
            unJoinedPie = laterPie;
            beforeIncreDeriveFromIntermJoin(ParentNode, joinedPie, unJoinedPie, earlyPie, laterPie, intermNewTab, intermOldTab, leafNode.getBefCol().getNewIEPTable(), leafNode.getBefCol().getIEPTable());
        }
        // 2. Join on the latter predicate: First join on the latter predicate, then derive the former predicate using the IE list
        else if (keyPredSet.contains(laterPie)) {
            joinedPie = laterPie;
            unJoinedPie = earlyPie;
            LinkList<IE> earlyList = leafNode.formerIEList;
            beforeIncreDeriveFromIEListJoin(ParentNode, joinedPie, unJoinedPie, earlyPie, laterPie, intermNewTab, intermOldTab, leafNode.getBefCol().getNewIEPTable(), leafNode.getBefCol().getIEPTable(), earlyList);
        } else {
            throw new IllegalStateException("Unexpected state: No matching condition for join logic.");
        }
    }

    // Handle merging intermediate nodes when deriving the "after" results.
    // This method processes different join cases based on key predicates.
    private void mergeIntermedNodeDerivingAfter(TreeNode ParentNode, TreeNode leafNode, Set<EBA> keyPredSet, EBA earlyPie, EBA laterPie, Table intermNewTab, Table intermOldTab) {

        EBA joinedPie;
        EBA unJoinedPie;

        // 3. Join on both: First join on the latter predicate, then derive the former predicate using the intermTable and IE list
        if (keyPredSet.contains(earlyPie) && keyPredSet.contains(laterPie)) {
            increDeriveFromIntermAndIEListJoin(ParentNode, earlyPie, laterPie, intermNewTab, intermOldTab, leafNode.getBefCol().getNewIEPTable(), leafNode.getBefCol().getIEPTable());
        }
        // 1. Join on the former predicate: First traverse the intermTable, then derive the former predicate
        else if (keyPredSet.contains(earlyPie)) {
            joinedPie = earlyPie;
            unJoinedPie = laterPie;
            afterIncreDeriveFromIntermJoin(ParentNode, joinedPie, unJoinedPie, earlyPie, laterPie, intermNewTab, intermOldTab, leafNode.getAftCol().getNewIEPTable(), leafNode.getAftCol().getIEPTable());
        }
        // 2. Join on the latter predicate: First join on the latter predicate, then derive the former predicate using the IE list
        else if (keyPredSet.contains(laterPie)) {
            joinedPie = laterPie;
            unJoinedPie = earlyPie;
            LinkList<IE> earlyList = leafNode.latterIEList;
            afterIncreDeriveFromIEListJoin(ParentNode, joinedPie, unJoinedPie, earlyPie, laterPie, intermNewTab, intermOldTab, leafNode.getAftCol().getNewIEPTable(), leafNode.getAftCol().getIEPTable(), earlyList);
        } else {
            throw new IllegalStateException("Unexpected state: No matching condition for join logic.");
        }
    }

    // Perform an incremental derivation and join between the intermediate table and the leaf node's new and old tables.
    private void increDeriveFromIntermAndIEListJoin(TreeNode ParentNode, EBA earlyPie, EBA laterPie, Table intermNewTab, Table intermOldTab, Table leafNewTab, Table leafOldTab) {
        long intermNewSize = intermNewTab.getSize();
        long leafNewSize = leafNewTab.getSize();

        // Ensure that leafNewSize is not greater than 1, as it represents a one-to-one relationship
        if (leafNewSize > 1) {
            throw new IllegalArgumentException("leafNewSize should not be greater than 1. Found: " + leafNewSize);
        }

        LinkList<Row>.Node leafNewPtr = leafNewTab.getRows().getHead();
        LinkList<Row>.Node leafOldPtr = leafOldTab.getRows().getHead();
        String earlyCol = EBA2String.get(earlyPie) + ".ST";
        String laterCol = EBA2String.get(laterPie) + ".ST";

        // If the leaf node has a new row, perform the join with the intermediate table
        if (leafNewSize != 0) {
            Row leafRow = leafNewTab.getRows().getHead().getData();
            // Intermediary table new JOIN leaf node new (N:1)
            deriveIntermAndIEListAndJoin_IntermTab_LeafRow(ParentNode, earlyCol, laterCol, intermNewTab, leafRow);
            // Intermediary table old JOIN leaf node new (N:1)
            deriveIntermAndIEListAndJoin_IntermTab_LeafRow(ParentNode, earlyCol, laterCol, intermOldTab, leafRow);
        }

        // If the intermediate table and leaf node old table have rows, perform the join (N:K)
        if (intermNewSize != 0 && leafOldTab.getSize() != 0) {
            // Split the operation into K (N:1) joins
            LinkList<Row>.Node leafRowPtr = leafOldTab.getRows().getTail();
            while (leafRowPtr != null) {
                deriveIntermAndIEListAndJoin_IntermTab_LeafRow(ParentNode, earlyCol, laterCol, intermNewTab, leafRowPtr.getData());
                leafRowPtr = leafRowPtr.prev;
            }
        }
    }
    // Derive intermediate rows and join them with the leaf row from the intermediate table.
    // This method compares the time data for the join condition and adds the joined rows to the new table.
    private void deriveIntermAndIEListAndJoin_IntermTab_LeafRow(TreeNode ParentNode, String earlyCol, String laterCol, Table intermTab, Row leafRow) {
        LinkList<Row>.Node intermPtr = intermTab.getRows().getTail();  // Start from the tail of the intermediate table's rows
        while (intermPtr != null && intermPtr.getData().getTimeData().get(laterCol) >= leafRow.getTimeData().get(laterCol)) {
            Row intermRow = intermPtr.getData();
            // If the later columns are equal, proceed to join on the full key
            if (intermRow.getTimeData().get(laterCol).equals(leafRow.getTimeData().get(laterCol))) {
                // Only derive if the early column in the intermediate row is less than or equal to the leaf row's early column
                if (intermRow.getTimeData().get(earlyCol).longValue() <= leafRow.getTimeData().get(earlyCol).longValue()) {
                    Row reindexRow = new Row(intermRow.getTimeData(), node2JoinedCols.get(ParentNode), intermRow.getSource(), intermRow.getTriggerTime(), ParentNode != root);
                    ParentNode.newT.addRow(reindexRow);  // Add the reindexed row to the parent node's new table
                }
            }
            intermPtr = intermPtr.prev;  // Move to the previous row in the intermediate table
        }
    }

    // Incrementally derive results from the intermediate table and join them with the leaf row for the "after" case.
    private void afterIncreDeriveFromIEListJoin(TreeNode ParentNode, EBA joinedPie, EBA unJoinedPie, EBA earlyPie, EBA laterPie, Table intermNewTab, Table intermOldTab, Table leafNewTab, Table leafOldTab, LinkList<IE> earlyIEList) {
        long intermNewSize = intermNewTab.getSize();
        long leafNewSize = leafNewTab.getSize();
        if (leafNewSize > 1) {
            throw new IllegalArgumentException("leafNewSize should not be greater than 1. Found: " + leafNewSize);
        }

        String unJoinedCol = EBA2String.get(unJoinedPie) + ".ST";

        if (leafNewSize != 0) {
            Row leafRow = leafNewTab.getRows().getHead().getData();
            // Intermediary table new JOIN leaf node new (N:1)
            deriveIEListAndJoin_IntermTab_LeafRow(ParentNode, earlyIEList, unJoinedCol, intermNewTab, leafRow);
            // Intermediary table old JOIN leaf node new (N:1)
            deriveIEListAndJoin_IntermTab_LeafRow(ParentNode, earlyIEList, unJoinedCol, intermOldTab, leafRow);
        }

        if (intermNewSize != 0 && leafOldTab.getSize() != 0) {
            // Intermediary table new JOIN leaf node old (N:K)
            LinkList<Row>.Node leafRowPtr = leafOldTab.getRows().getTail();
            while (leafRowPtr != null) {
                deriveIEListAndJoin_IntermTab_LeafRow(ParentNode, earlyIEList, unJoinedCol, intermNewTab, leafRowPtr.getData());
                leafRowPtr = leafRowPtr.prev;
            }
        }
    }

    // Incrementally derive results from the intermediate table and join them with the leaf row for the "before" case.
    private void beforeIncreDeriveFromIEListJoin(TreeNode ParentNode, EBA joinedPie, EBA unJoinedPie, EBA earlyPie, EBA laterPie, Table intermNewTab, Table intermOldTab, Table leafNewTab, Table leafOldTab, LinkList<IE> earlyIEList) {
        long intermNewSize = intermNewTab.getSize();
        long leafNewSize = leafNewTab.getSize();
        if (leafNewSize > 1) {
            throw new IllegalArgumentException("leafNewSize should not be greater than 1. Found: " + leafNewSize);
        }

        String unJoinedCol = EBA2String.get(unJoinedPie) + ".ST";

        if (leafNewSize != 0) {
            Row leafRow = leafNewTab.getRows().getHead().getData();
            // Intermediary table new JOIN leaf node new (N:1)
            deriveIEListAndJoin_IntermTab_LeafRow(ParentNode, earlyIEList, unJoinedCol, intermNewTab, leafRow);
            // Intermediary table old JOIN leaf node new (N:1)
            deriveIEListAndJoin_IntermTab_LeafRow(ParentNode, earlyIEList, unJoinedCol, intermOldTab, leafRow);
        }

        if (intermNewSize != 0 && leafOldTab.getSize() != 0) {
            // Intermediary table new JOIN leaf node old (N:K)
            LinkList<Row>.Node leafRowPtr = leafOldTab.getRows().getTail();
            while (leafRowPtr != null) {
                deriveIEListAndJoin_IntermTab_LeafRow(ParentNode, earlyIEList, unJoinedCol, intermNewTab, leafRowPtr.getData());
                leafRowPtr = leafRowPtr.prev;
            }
        }
    }

    // Derive results by joining the intermediate table with the leaf row and the IE list.
    private void deriveIEListAndJoin_IntermTab_LeafRow(TreeNode ParentNode, LinkList<IE> earlyIEList, String unJoinedCol, Table intermTab, Row leafRow) {
        String leafIndex = leafRow.getIndexKey();
        List<Row> intermRows = intermTab.getHashIndex().get(leafIndex);  // Get rows from the intermediate table that match the leaf row's index

        if (intermRows != null) {
            for (Row intermRow : intermRows) {
                Row joinedRow = intermRow.join(leafRow, node2JoinedCols.get(ParentNode), ParentNode != root);  // Join the intermediate row with the leaf row
                ParentNode.newT.addRow(joinedRow);  // Add the joined row to the new table of the parent node

                // Join on full data key and derive in the IE list (1:1 => 1:M)
                LinkList<IE>.Node ieNode = earlyIEList.getTail();  // Start from the tail of the IE list
                // Skip already merged IE nodes
                while (ieNode != null && ieNode.getData().getStartTime() >= leafRow.getTimeData().get(unJoinedCol)) {
                    ieNode = ieNode.prev;
                }

                long derivedIEcnt = 0;  // Count the number of derived IE entries
                while (ieNode != null) {
                    // Join the row with the IE entry
                    joinRowWithIE(ParentNode, intermRow, unJoinedCol, ieNode.getData());
                    ieNode = ieNode.prev;
                    derivedIEcnt++;  // Increment the derived IE count
                }

                // Log the count of derived IE entries
                if (derivedIEcnt > 0) {
                    logger.debug("derivedIEcnt: " + derivedIEcnt);
                }
            }
        }
    }

    // Build a new row by joining the intermediate row with the IE entry from the leaf row.
    private void joinRowWithIE(TreeNode ParentNode, Row intermRow, String addedPieName, IE ie) {
        Map<String, Long> timeData = new HashMap<>(intermRow.getTimeData());
        timeData.put(addedPieName, ie.getStartTime());  // Add the start time of the IE entry to the time data

        Set<Expirable> source = new HashSet<>(intermRow.getSource());
        source.add(ie);  // Add the IE entry to the source set

        long minTrig = min(ie.getStartTime(), intermRow.getTriggerTime());  // Determine the minimum trigger time
        Row newRow = new Row(timeData, node2JoinedCols.get(ParentNode), source, minTrig, ParentNode != root);  // Create the new joined row
        ParentNode.newT.addRow(newRow);  // Add the new row to the parent node's new table
    }
    // Derives results incrementally by joining the intermediate table with the leaf table, specifically handling the "after" case.
    // Joins the "New" and "Old" intermediate tables with the "New" and "Old" leaf tables respectively.
    private void afterIncreDeriveFromIntermJoin(TreeNode ParentNode, EBA joinedPie, EBA unJoinedPie, EBA earlyPie, EBA laterPie, Table intermNewTab, Table intermOldTab, Table leafNewTab, Table leafOldTab) {
        long intermNewSize = intermNewTab.getSize();
        long leafNewSize = leafNewTab.getSize();

        // Ensure leafNewSize is not greater than 1
        if (leafNewSize > 1) {
            throw new IllegalArgumentException("leafNewSize should not be greater than 1. Found: " + leafNewSize);
        }

        // Get pointers to the head of the leaf tables
        LinkList<Row>.Node leafNewPtr = leafNewTab.getRows().getHead();
        LinkList<Row>.Node leafOldPtr = leafOldTab.getRows().getHead();

        String joinedCol = EBA2String.get(joinedPie) + ".ST";
        String unJoinedCol = EBA2String.get(unJoinedPie) + ".ST";

        if (leafNewSize != 0) {
            // Join intermedia.New with leaf.New (N:1)
            deriveIntermAndJoin_Interm_LeafNew(ParentNode, joinedCol, unJoinedCol, intermNewTab, leafNewPtr);
            // Join intermedia.Old with leaf.New (N:1)
            deriveIntermAndJoin_Interm_LeafNew(ParentNode, joinedCol, unJoinedCol, intermOldTab, leafNewPtr);
        }

        if (intermNewSize != 0 && leafOldTab.getSize() != 0) {
            // Join intermedia.New with leaf.Old (N:M)
            deriveIntermAndJoin_IntermNew_LeafOld(ParentNode, intermNewTab, joinedCol, unJoinedCol, leafOldPtr);
        }
    }

    // Derives results incrementally by joining the intermediate table with the leaf table, specifically handling the "before" case.
    // Joins the "New" and "Old" intermediate tables with the "New" and "Old" leaf tables respectively.
    private void beforeIncreDeriveFromIntermJoin(TreeNode ParentNode, EBA joinedPie, EBA unJoinedPie, EBA earlyPie, EBA laterPie, Table intermNewTab, Table intermOldTab, Table leafNewTab, Table leafOldTab) {
        long intermNewSize = intermNewTab.getSize();
        long leafNewSize = leafNewTab.getSize();

        // Ensure leafNewSize is not greater than 1
        if (leafNewSize > 1) {
            throw new IllegalArgumentException("leafNewSize should not be greater than 1. Found: " + leafNewSize);
        }

        // Get pointers to the head of the leaf tables
        LinkList<Row>.Node leafNewPtr = leafNewTab.getRows().getHead();
        LinkList<Row>.Node leafOldPtr = leafOldTab.getRows().getHead();

        String joinedCol = EBA2String.get(joinedPie) + ".ST";
        String unJoinedCol = EBA2String.get(unJoinedPie) + ".ST";

        if (leafNewSize != 0) {
            // Join intermedia.New with leaf.New (N:1)
            deriveIntermAndJoin_Interm_LeafNew(ParentNode, joinedCol, unJoinedCol, intermNewTab, leafNewPtr);
            // Join intermedia.Old with leaf.New (N:1)
            deriveIntermAndJoin_Interm_LeafNew(ParentNode, joinedCol, unJoinedCol, intermOldTab, leafNewPtr);
        }

        if (intermNewSize != 0 && leafOldTab.getSize() != 0) {
            // Join intermedia.New with leaf.Old (N:M)
            deriveIntermAndJoin_IntermNew_LeafOld(ParentNode, intermNewTab, joinedCol, unJoinedCol, leafOldPtr);
        }
    }

    // Derives and joins intermediate rows from the intermediate table to the leaf row (New leaf) in the "after" case.
    private void deriveIntermAndJoin_Interm_LeafNew(TreeNode ParentNode, String joinedCol, String unJoinedCol, Table intermTab, LinkList<Row>.Node leafPtr) {
        if (intermTab.getSize() != 0) {
            LinkList<Row>.Node intermNode = intermTab.getRows().getHead();
            while (intermNode != null) {
                long intermTime = intermNode.getData().getTimeData().get(joinedCol);
                if (intermTime <= leafPtr.getData().getTimeData().get(joinedCol)) {
                    // Join at most once every round
                    joinTwoRows(ParentNode, intermNode.getData(), unJoinedCol, leafPtr.getData());
                }
                intermNode = intermNode.next;
            }
        }
    }

    // Derives and joins intermediate rows from the intermediate table to the leaf row (Old leaf) in the "after" case.
    private void deriveIntermAndJoin_IntermNew_LeafOld(TreeNode ParentNode, Table intermTab, String joinedCol, String unJoinedCol, LinkList<Row>.Node leafPtr) {
        if (intermTab.getSize() != 0) {
            LinkList<Row>.Node intermNode = intermTab.getRows().getHead();
            while (intermNode != null) {
                long intermTime = intermNode.getData().getTimeData().get(joinedCol);
                if (intermTime <= leafPtr.getData().getTimeData().get(joinedCol)) {
                    // First: Find the steer, search backward
                    while (leafPtr != null && leafPtr.getData().getTimeData().get(joinedCol) > intermTime) {
                        leafPtr = leafPtr.prev;
                    }
                    // Second: Build the rows, build forward
                    while (leafPtr != null) {
                        joinTwoRows(ParentNode, intermNode.getData(), unJoinedCol, leafPtr.getData());
                        leafPtr = leafPtr.next;
                    }
                }
                intermNode = intermNode.next;
            }
        }
    }
    // Builds a new Row by joining two rows: intermRow is the base row, and addedPieName from the leafRow is added.
    // Combines time data and sources from both rows and computes the minimum trigger time for the new row.
    private void joinTwoRows(TreeNode ParentNode, Row intermRow, String addedPieName, Row leafRow) {
        // Copy the time data from the intermRow and add the time data from the leafRow under addedPieName.
        Map<String, Long> timeData = new HashMap<>(intermRow.getTimeData());
        timeData.put(addedPieName, leafRow.getTimeData().get(addedPieName));

        // Combine the source data from both rows.
        Set<Expirable> source = new HashSet<>(intermRow.getSource());
        source.addAll(leafRow.getSource());

        // Determine the minimum trigger time between the two rows.
        long minTrig = min(leafRow.getTriggerTime(), intermRow.getTriggerTime());

        // Create the new row with the combined time data, source, and minimum trigger time.
        Row newRow = new Row(timeData, node2JoinedCols.get(ParentNode), source, minTrig, ParentNode != root);

        // Add the new row to the ParentNode's new table.
        ParentNode.newT.addRow(newRow);
    }

    // Performs incremental natural joins between the intermediate tables and the leaf tables.
    // Joins intermedia.New with leaf.Old, intermedia.Old with leaf.New, and intermedia.New with leaf.New.
    private void incrementalNaturalJoin(TreeNode ParentNode, Table intermNewTab, Table intermOldTab, Table leafNewTab, Table leafOldTab) {
        long mergedNewSize = intermNewTab.getSize();
        long leafNewSize = leafNewTab.getSize();

        // Join intermedia.New with leaf.Old if intermNewTab is not empty
        if (mergedNewSize != 0) {
            // Perform a hash join on intermNewTab and leafOldTab
            Table IntermNew_LeafOld = HashJoiner.hashJoin(intermNewTab, leafOldTab, node2JoinedCols.get(ParentNode), ParentNode != root);
            // Concatenate the result to the ParentNode's new table
            ParentNode.newT.concatenate(IntermNew_LeafOld);
        }

        // Join intermedia.Old with leaf.New if leafNewTab is not empty
        if (leafNewSize != 0) {
            // Perform a hash join on leafNewTab and intermOldTab
            Table IntermOld_LeafNew = HashJoiner.hashJoin(leafNewTab, intermOldTab, node2JoinedCols.get(ParentNode), ParentNode != root);
            // Concatenate the result to the ParentNode's new table
            ParentNode.newT.concatenate(IntermOld_LeafNew);
        }

        // Join intermedia.New with leaf.New if both intermNewTab and leafNewTab are not empty
        if (mergedNewSize * leafNewSize != 0) {
            // Perform a hash join on intermNewTab and leafNewTab
            Table IntermNew_LeafNew = HashJoiner.hashJoin(intermNewTab, leafNewTab, node2JoinedCols.get(ParentNode), ParentNode != root);
            // Concatenate the result to the ParentNode's new table
            ParentNode.newT.concatenate(IntermNew_LeafNew);
        }
    }


}
