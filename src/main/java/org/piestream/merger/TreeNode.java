package org.piestream.merger;

import org.piestream.engine.MPIEPair;
import org.piestream.engine.Window;
import org.piestream.events.PointEvent;
import org.piestream.parser.MPIEPairSource;
import org.piestream.piepair.IE;
import org.piestream.piepair.IEP;
import org.piestream.piepair.PIEPair;
import org.piestream.piepair.TemporalRelations;
import org.piestream.piepair.eba.EBA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TreeNode {
    private static final Logger logger = LoggerFactory.getLogger(TreeNode.class);
    final Set<EBA> predSet;      // Set of predicates associated with the node
    final Set<EBA> keyPredSet;   // Set of key predicates shared between nodes (join key)
    TreeNode parent;             // Parent node in the tree
    TreeNode brother;            // Brother node used during merging process
    TreeNode left;               // Left child node
    TreeNode right;              // Right child node
    final MPIEPairSource source; // Source object used when creating the node
    int height;                  // Height of the node in the tree
    final IEPCol Col;            // Queue for detection results of specified exact relations
    Table T;                     // Table used to store node information
    Table newT;
    final IEPCol befCol;         // Queue for before relation detection results
    final IEPCol aftCol;         // Queue for after relation detection results
    final LinkList<IE> formerIEList;    // Interval event queue used to assist in detecting before relations
    final LinkList<IE> latterIEList;    // Interval event queue used to assist in detecting after relations
    final boolean isLeaf;        // Flag indicating if the node is a leaf node
    final boolean hasBefore;     // Flag indicating if the node contains a before relation
    final boolean hasAfter;      // Flag indicating if the node contains an after relation
    final Window window;
    MPIEPair mpp;      // Stores whether the node contains after relations

    long resCount;
    long processTime;

    /**
     * Initializes a TreeNode with the given parameters.
     * This constructor is responsible for setting up the node as either a leaf or non-leaf node,
     * including setting various attributes based on the provided parameters and conditions.
     * If the node is a leaf, it initializes additional attributes for handling related data.
     *
     * @param predSet Set of predicate elements associated with the node.
     * @param keyPredSet Set of key predicates that may be used for the node's operations.
     * @param left Left child of the node.
     * @param right Right child of the node.
     * @param parent Parent of the node.
     * @param brother Sibling node.
     * @param source The source object containing information related to the node.
     * @param height Height of the node in the tree.
     * @param isLeaf Flag indicating if the node is a leaf node.
     * @param window The window associated with the node.
     * @param EBA2String Map linking EBA objects to their string representations.
     */
    public TreeNode(Set<EBA> predSet, Set<EBA> keyPredSet, TreeNode left, TreeNode right,
                    TreeNode parent, TreeNode brother, MPIEPairSource source, int height,
                    boolean isLeaf, Window window, Map<EBA, String> EBA2String) {
        this.window = window;
        this.predSet = predSet;
        this.keyPredSet = keyPredSet;
        this.left = left;
        this.right = right;
        this.parent = parent;
        this.brother = brother;
        this.source = source;
        this.height = height;
        this.isLeaf = isLeaf;
        this.resCount = 0;
        this.processTime = 0;
        Window bef_aftWindow = window;

        if (isLeaf) {
            this.Col = new IEPCol(window, EBA2String);
            this.T = null;
            this.newT = null;

            if (source.isHasAfterRel()) {
                this.hasAfter = true;
                this.latterIEList = new LinkList<>(window);
                this.aftCol = new IEPCol(bef_aftWindow, EBA2String);
            } else {
                this.hasAfter = false;
                this.latterIEList = null;
                this.aftCol = null;
            }

            if (source.isHasBeforeRel()) {
                this.hasBefore = true;
                this.formerIEList = new LinkList<>(window);
                this.befCol = new IEPCol(bef_aftWindow, EBA2String);
            } else {
                this.hasBefore = false;
                this.formerIEList = null;
                this.befCol = null;
            }
        } else {
            this.T = new Table(bef_aftWindow);
            this.newT = new Table(bef_aftWindow);
            this.Col = null;
            this.hasAfter = false;
            this.hasBefore = false;
            this.formerIEList = null;
            this.latterIEList = null;
            this.befCol = null;
            this.aftCol = null;
        }
    }

    /**
     * This constructor initializes a TreeNode as a leaf node.
     * It calls the main constructor with the `isLeaf` flag set to true, setting up additional attributes for leaf-specific behavior.
     *
     * @param predSet Set of predicate elements associated with the node.
     * @param source The source object containing information related to the node.
     * @param window The window associated with the node.
     * @param EBA2String Map linking EBA objects to their string representations.
     */
    public TreeNode(Set<EBA> predSet, MPIEPairSource source, Window window, Map<EBA, String> EBA2String) {
        this(predSet, new HashSet<>(), null, null, null, null, source, 0, true, window, EBA2String);
    }

    /**
     * This constructor initializes a TreeNode as a non-leaf (parent) node.
     * It calls the main constructor with the `isLeaf` flag set to false, setting up additional attributes for parent-specific behavior.
     *
     * @param predSet Set of predicate elements associated with the node.
     * @param keyPredSet Set of key predicates for the node.
     * @param left Left child of the node.
     * @param right Right child of the node.
     * @param height Height of the node in the tree.
     * @param window The window associated with the node.
     * @param EBA2String Map linking EBA objects to their string representations.
     */
    public TreeNode(Set<EBA> predSet, Set<EBA> keyPredSet, TreeNode left, TreeNode right, int height, Window window, Map<EBA, String> EBA2String) {
        this(predSet, keyPredSet, left, right, null, null, null, height, false, window, EBA2String);
    }

// Getter method for predSet
    /**
     * Returns the set of predicates associated with the node.
     *
     * @return The set of predicates.
     */
    public Set<EBA> getPredSet() {
        return predSet;
    }

// Getter method for resCount
    /**
     * Returns the current result count for the node.
     *
     * @return The result count.
     */
    public long getResCount() {
        return resCount;
    }

// Adds a value to the result count
    /**
     * Adds the specified value to the node's result count.
     *
     * @param n The value to add to the result count.
     */
    public void addResCount(long n) {
        this.resCount += n;
    }

    /**
     * Adds the specified value to the node's processing time.
     *
     * @param n The value to add to the processing time.
     */
    public void addProcessTime(long n) {
        this.processTime += n;
    }



    /**
     * Sets the MPIEPair for the node.
     *
     * @param mpp The MPIEPair object to set.
     */
    public void setMPIEPair(MPIEPair mpp) {
        this.mpp = mpp;
    }

    /**
     * Returns the set of key predicates associated with the node.
     *
     * @return The set of key predicates.
     */
    public Set<EBA> getKeyPredSet() {
        return keyPredSet;
    }

    /**
     * Returns the left child of the node.
     *
     * @return The left child node.
     */
    public TreeNode getLeft() {
        return left;
    }

    /**
     * Sets the left child of the node.
     *
     * @param left The left child node to set.
     */
    public void setLeft(TreeNode left) {
        this.left = left;
    }

    /**
     * Returns the right child of the node.
     *
     * @return The right child node.
     */
    public TreeNode getRight() {
        return right;
    }

    /**
     * Sets the right child of the node.
     *
     * @param right The right child node to set.
     */
    public void setRight(TreeNode right) {
        this.right = right;
    }

    /**
     * Returns the source associated with the node.
     *
     * @return The source object of the node.
     */
    public MPIEPairSource getSource() {
        return source;
    }

    /**
     * Returns the height of the node in the tree.
     *
     * @return The height of the node.
     */
    public int getHeight() {
        return height;
    }

    /**
     * Sets the height of the node in the tree.
     *
     * @param height The height value to set for the node.
     */
    public void setHeight(int height) {
        this.height = height;
    }

    /**
     * Returns the IEPCol associated with the node.
     *
     * @return The IEPCol object associated with the node.
     */
    public IEPCol getCol() {
        return this.Col;
    }

    /**
     * Returns the Table object associated with the node.
     *
     * @return The Table object.
     */
    public Table getT() {
        return T;
    }

    /**
     * Returns the new Table object associated with the node.
     *
     * @return The new Table object.
     */
    public Table getNewT() {
        return newT;
    }

    /**
     * Sets the Table object for the node.
     *
     * @param T The Table object to set.
     */
    public void setTable(Table T) {
        this.T = T;
    }

    /**
     * Returns the before column (IEPCol) associated with the node.
     *
     * @return The before column (IEPCol) object.
     */
    public IEPCol getBefCol() {
        return befCol;
    }

    /**
     * Returns the after column (IEPCol) associated with the node.
     *
     * @return The after column (IEPCol) object.
     */
    public IEPCol getAftCol() {
        return aftCol;
    }

    /**
     * Returns the list of former IEs (LinkList).
     *
     * @return The LinkList containing former IEs.
     */
    public LinkList<IE> getFormerIEList() {
        return formerIEList;
    }

    /**
     * Returns the list of latter IEs (LinkList).
     *
     * @return The LinkList containing latter IEs.
     */
    public LinkList<IE> getLatterIEList() {
        return latterIEList;
    }

    /**
     * Returns whether the node is a leaf node.
     *
     * @return true if the node is a leaf, false otherwise.
     */
    public boolean isLeaf() {
        return isLeaf;
    }

    /**
     * Returns whether the node has after relations.
     *
     * @return true if the node has after relations, false otherwise.
     */
    public boolean isHasAfter() {
        return hasAfter;
    }

    /**
     * Returns whether the node has before relations.
     *
     * @return true if the node has before relations, false otherwise.
     */
    public boolean isHasBefore() {
        return hasBefore;
    }


    /**
     * Derives the before and after temporal relationships based on the flag `needDerivePrev`.
     * If true, it will attempt to derive previous before/after relationships from the existing event list.
     *
     * @param needDerivePrev A flag indicating whether previous relationships should be derived.
     */
    public void deriveBeforeAfterRel(boolean needDerivePrev) {
//        needDerivePrev=true;
        // Only bottomLeaf needs to derive All
        if (hasBefore) {
            if (mpp.isHasNewLatterIE()) {
                PointEvent latterPieStart = mpp.getLatterPieStart();
                LinkList.Node tail = deriveClosestBefIEP(latterPieStart);
                if (needDerivePrev) {
                    derivePreviousBefFromIE(tail, latterPieStart);
                }
            }
        }
        if (hasAfter) {
            if (mpp.isHasNewFormerIE()) {
                PointEvent formerPieStart = mpp.getFormerPieStart();
                LinkList.Node tail = deriveClosestAftIEP(formerPieStart);
                if (needDerivePrev) {
                    derivePreviousAftFromIE(tail, formerPieStart);
                }
            }
        }
    }


    /**
     * Derives the closest before temporal relationship (IEP) for the given `latterPieStart` event.
     *
     * @param latterPieStart The PointEvent marking the start of the latter event for the "before" relation.
     * @return The last valid node in the formerIEList that satisfies the temporal condition, or null if no valid node.
     */
    private LinkList<IE>.Node deriveClosestBefIEP(PointEvent latterPieStart) {
        LinkList<IE>.Node tail = formerIEList.getTail();
        if (tail != null) {
            IE lastFormerIE = tail.getData();
            // Find the last FormerIE that could just satisfy before()
            // We are sure that: lastFormerIE.getStartTime() <= latterPieStart.getTimestamp()
            if (lastFormerIE.getStartTime() == latterPieStart.getTimestamp()) { // starts, started-by
                tail = tail.prev;
            } else if (lastFormerIE.getEndTime() == null) { // lastFormerIE is not completed
                tail = tail.prev;
            } else if (lastFormerIE.getEndTime() == latterPieStart.getTimestamp()) { // meets
                tail = tail.prev;
            } else { // followed-by, ideal case
            }
            lastFormerIE = tail.getData();
            IEP newIEP = new IEP(TemporalRelations.AllenRel.BEFORE, mpp.getFormerPred(), mpp.getLatterPred(),
                    lastFormerIE.getStartEvent(), latterPieStart, lastFormerIE.getEndEvent(), null,
                    lastFormerIE.getStartTime(), latterPieStart.getTimestamp(), latterPieStart, latterPieStart.getTimestamp());
            befCol.setTriggerMSG(newIEP);
            logger.debug("trigger(key): "
                    + newIEP.getRelation() + "(" + newIEP.getFormerStartTime() + "," + newIEP.getLatterStartTime() + ")");
            return tail.prev;
        }
        return null;
    }


    /**
     * Derives previous before temporal relationships (IEPs) starting from the given tail node and the `latterPieStart` event.
     *
     * @param tail The starting node in the formerIEList from which to derive previous before IEPs.
     * @param latterPieStart The PointEvent marking the start of the latter event for the "before" relation.
     */
    public void derivePreviousBefFromIE(LinkList<IE>.Node tail, PointEvent latterPieStart) {
        while (tail != null) {
            IE lastFormerIE = tail.getData();
            befCol.setTriggerMSG(new IEP(TemporalRelations.AllenRel.BEFORE, mpp.getFormerPred(), mpp.getLatterPred(),
                    lastFormerIE.getStartEvent(), latterPieStart, lastFormerIE.getEndEvent(), null,
                    lastFormerIE.getStartTime(), latterPieStart.getTimestamp(), latterPieStart, latterPieStart.getTimestamp()));
            tail = tail.prev;
        }
    }


    /**
     * Derives the closest after temporal relationship (IEP) for the given `formerPieStart` event.
     *
     * @param formerPieStart The PointEvent marking the start of the former event for the "after" relation.
     * @return The last valid node in the latterIEList that satisfies the temporal condition, or null if no valid node.
     */
    private LinkList<IE>.Node deriveClosestAftIEP(PointEvent formerPieStart) {
        LinkList<IE>.Node tail = latterIEList.getTail();
        if (tail != null) {
            IE lastLatterIE = tail.getData();
            // Find the last LatterIE that could just satisfy after()
            // We are sure that: lastLatterIE.getStartTime() <= formerPieStart.getTimestamp()
            if (lastLatterIE.getStartTime() == formerPieStart.getTimestamp()) { // starts, started-by
                tail = tail.prev;
            } else if (lastLatterIE.getEndTime() == null) { // lastLatterIE is not completed
                tail = tail.prev;
            } else if (lastLatterIE.getEndTime() == formerPieStart.getTimestamp()) { // met-by
                tail = tail.prev;
            } else { // follow, ideal case
            }
            lastLatterIE = tail.getData();
            IEP newIEP = new IEP(TemporalRelations.AllenRel.AFTER, mpp.getFormerPred(), mpp.getLatterPred(),
                    formerPieStart, lastLatterIE.getStartEvent(), null, lastLatterIE.getEndEvent(),
                    formerPieStart.getTimestamp(), lastLatterIE.getStartTime(), formerPieStart, formerPieStart.getTimestamp());
            aftCol.setTriggerMSG(newIEP);
            logger.debug("trigger(key): "
                    + newIEP.getRelation() + "(" + newIEP.getFormerStartTime() + "," + newIEP.getLatterStartTime() + ")");
            return tail.prev;
        }
        return null;
    }


    /**
     * Derives previous after temporal relationships (IEPs) starting from the given tail node and the `formerPieStart` event.
     *
     * @param tail The starting node in the latterIEList from which to derive previous after IEPs.
     * @param formerPieStart The PointEvent marking the start of the former event for the "after" relation.
     */
    public void derivePreviousAftFromIE(LinkList<IE>.Node tail, PointEvent formerPieStart) {
        while (tail != null) {
            IE lastLatterIE = tail.getData();
            aftCol.setTriggerMSG(new IEP(TemporalRelations.AllenRel.AFTER, mpp.getFormerPred(), mpp.getLatterPred(),
                    formerPieStart, lastLatterIE.getStartEvent(), null, lastLatterIE.getEndEvent(),
                    formerPieStart.getTimestamp(), lastLatterIE.getStartTime(), formerPieStart, formerPieStart.getTimestamp()));
            tail = tail.prev;
        }
    }

    /**
     * Calculates the average processing time based on the total processing time and the result count.
     *
     * @return The average processing time.
     */
    public double getAVGprocessTime() {
        return (double) this.processTime / (double) this.resCount;
    }


}