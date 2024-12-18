package org.piestream.engine;

import org.piestream.events.PointEvent;
import org.piestream.merger.LinkList;
import org.piestream.merger.TreeNode;
import org.piestream.piepair.*;
import org.piestream.piepair.dfa.Alphabet;
import org.piestream.piepair.eba.EBA;
import org.piestream.merger.IEPCol;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * The MPIEPair class represents a pattern matching mechanism that uses PIEPair objects
 * for precise temporal relationships detection between events. It manages the transitions
 * of different states for temporal relations and processes incoming events according
 * to defined temporal relations.
 *
 * It includes the following functionalities:
 * 1. Classifies and processes events based on temporal relations.
 * 2. Handles transitions for both former and latter PIE events.
 * 3. Provides methods to manage PIEPair objects and their interactions.
 */
public class MPIEPair {
    private final Set<TemporalRelations.PreciseRel> relations;  // List of precise temporal relations
    private final EventClassifier classifier;                    // Classifier to classify incoming events
    private final EBA formerPred;                                // Former EBA (Event Based Automaton)
    private final EBA latterPred;                                // Latter EBA
    private final int QCapacity;                                 // Queue capacity
    private final List<PIEPair> piePairs;                        // List of PIEPairs
    private Alphabet lastAlphabet;                               // Last alphabet state
    private Alphabet currentAlphabet;                            // Current alphabet state
    private PointEvent formerPieStart;                           // Start event for former PIE
    private PointEvent formerPieEnd;                             // End event for former PIE
    private PointEvent latterPieStart;                           // Start event for latter PIE
    private PointEvent latterPieEnd;                             // End event for latter PIE
    private final LinkList<IE> formerIEList;                           // List of former interval events
    private final LinkList<IE> latterIEList;                           // List of latter interval events
    private final TreeNode node;                                  // Tree node representing the pattern
    private final boolean hasBefore;                             // Flag indicating the presence of a "before" relationship
    private final boolean hasAfter;                              // Flag indicating the presence of an "after" relationship
    private boolean hasNewFormerIE;                              // Flag indicating new former interval event
    private boolean hasNewLatterIE;                              // Flag indicating new latter interval event

    /**
     * Constructor to initialize the MPIEPair object and create PIEPairs for the given relations.
     *
     * @param relations Set of precise temporal relations
     * @param formerPred Former EBA (Event-Based Automaton) to classify the events
     * @param latterPred Latter EBA to classify the events
     * @param node TreeNode representing the state of the pattern
     */
    public MPIEPair(Set<TemporalRelations.PreciseRel> relations, EBA formerPred, EBA latterPred, TreeNode node) {
        if (relations == null || formerPred == null || latterPred == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }
        this.relations =  relations;
        this.formerPred = formerPred;
        this.latterPred = latterPred;
        this.classifier = new EventClassifier(formerPred, latterPred);
        this.QCapacity = 0;
        this.piePairs = new ArrayList<>();
        this.node = node;
        this.node.setMPIEPair(this);
        this.hasBefore = node.isHasBefore();
        this.hasAfter = node.isHasAfter();
        this.hasNewFormerIE = false;
        this.hasNewLatterIE = false;

        for (TemporalRelations.PreciseRel relation : relations) {
            PIEPair pp = new PIEPair(relation, formerPred, latterPred, this);
            this.piePairs.add(pp);
        }

        if (node.isHasBefore()) {
            this.formerIEList = node.getFormerIEList();
        } else {
            this.formerIEList = null;
        }

        if (node.isHasAfter()) {
            this.latterIEList = node.getLatterIEList();
        } else {
            this.latterIEList = null;
        }
    }

    public boolean isHasNewFormerIE() {
        return hasNewFormerIE;
    }

    public boolean isHasNewLatterIE() {
        return hasNewLatterIE;
    }

    public PointEvent getFormerPieStart() {
        return formerPieStart;
    }

    public PointEvent getFormerPieEnd() {
        return formerPieEnd;
    }

    public PointEvent getLatterPieStart() {
        return latterPieStart;
    }

    public PointEvent getLatterPieEnd() {
        return latterPieEnd;
    }

    private void resetNewIE() {
        hasNewFormerIE = false;
        hasNewLatterIE = false;
    }

    /**
     * Processes an incoming PointEvent and updates the MPIEPair accordingly.
     * It classifies the event and manages transitions between states.
     *
     * @param event The incoming event to be processed
     */
    public void run(PointEvent event) {
        resetNewIE();
        Alphabet newAlphabet = classifier.classify(event);
        lastAlphabet = currentAlphabet;
        currentAlphabet = newAlphabet;
        recordIntervalEvent(event);

        for (PIEPair pp : piePairs) {
            pp.stepByPE(event, newAlphabet, formerPieStart, formerPieEnd, latterPieStart, latterPieEnd);
        }
    }

    /**
     * Returns the set of precise temporal relations that this MPIEPair is monitoring.
     *
     * @return Set of precise temporal relations
     */
    public Set<TemporalRelations.PreciseRel> getRelations() {
        return relations;
    }

    /**
     * Returns the former EBA.
     *
     * @return Former EBA
     */
    public EBA getFormerPred() {
        return formerPred;
    }

    /**
     * Returns the latter EBA.
     *
     * @return Latter EBA
     */
    public EBA getLatterPred() {
        return latterPred;
    }

    /**
     * Returns the queue capacity.
     *
     * @return Queue capacity
     */
    public int getQCapacity() {
        return QCapacity;
    }

    /**
     * Returns the IEPCol associated with the current TreeNode.
     *
     * @return IEPCol of the TreeNode
     */
    public IEPCol getCol() {
        return node.getCol();
    }

    /**
     * Records an event and updates the respective PIE start and end events.
     * Depending on the transition types, the corresponding PIE start and end events
     * are updated, and interval events are added to the respective lists.
     *
     * @param event The incoming event to be recorded
     */
    private void recordIntervalEvent(PointEvent event) {
        if (isFormerPieStartTransition()) {
            formerPieStart = event;
            formerPieEnd = null;
            hasNewFormerIE = true;
        }
        if (isFormerPieEndTransition()) {
            formerPieEnd = event;
            if (hasBefore) {
                formerIEList.safeAdd(new IE(formerPred, formerPieStart, formerPieEnd, event.getTimestamp()));
            }
            if (hasAfter) {
                node.getAftCol().updateCompletedMSG("former", formerPred, formerPieStart.getTimestamp(), formerPieEnd);
            }
        }
        if (isLatterPieStartTransition()) {
            latterPieStart = event;
            latterPieEnd = null;
            hasNewLatterIE = true;
        }
        if (isLatterPieEndTransition()) {
            latterPieEnd = event;
            if (hasAfter) {
                latterIEList.safeAdd(new IE(latterPred, latterPieStart, latterPieEnd, event.getTimestamp()));
            }
            if (hasBefore) {
                node.getBefCol().updateCompletedMSG("latter", latterPred, latterPieStart.getTimestamp(), latterPieEnd);
            }
        }
    }

    /**
     * Checks if the transition is the start of a former PIE.
     *
     * @return true if it is the start of a former PIE, false otherwise
     */
    public boolean isFormerPieStartTransition() {
        return (lastAlphabet == Alphabet.O || lastAlphabet == Alphabet.I || lastAlphabet == null) &&
                (currentAlphabet == Alphabet.Z || currentAlphabet == Alphabet.E);
    }

    /**
     * Checks if the transition is the end of a former PIE.
     *
     * @return true if it is the end of a former PIE, false otherwise
     */
    public boolean isFormerPieEndTransition() {
        return (lastAlphabet == Alphabet.Z || lastAlphabet == Alphabet.E) &&
                (currentAlphabet == Alphabet.O || currentAlphabet == Alphabet.I);
    }

    /**
     * Checks if the transition is the start of a latter PIE.
     *
     * @return true if it is the start of a latter PIE, false otherwise
     */
    public boolean isLatterPieStartTransition() {
        return (lastAlphabet == Alphabet.O || lastAlphabet == Alphabet.Z || lastAlphabet == null) &&
                (currentAlphabet == Alphabet.I || currentAlphabet == Alphabet.E);
    }

    /**
     * Checks if the transition is the end of a latter PIE.
     *
     * @return true if it is the end of a latter PIE, false otherwise
     */
    public boolean isLatterPieEndTransition() {
        return (lastAlphabet == Alphabet.I || lastAlphabet == Alphabet.E) &&
                (currentAlphabet == Alphabet.O || currentAlphabet == Alphabet.Z);
    }

    /**
     * Returns the list of all PIEPairs in this MPIEPair.
     *
     * @return List of PIEPairs
     */
    public List<PIEPair> getPiePairs() {
        return piePairs;
    }

    @Override
    public String toString() {
        return "MPIEPair{" +
                "relations=" + relations +
                ", formerPred=" + formerPred +
                ", latterPred=" + latterPred +
                '}';
    }
}
