package org.piestream.piepair;

import org.piestream.engine.MPIEPair;
import org.piestream.events.PointEvent;
import org.piestream.piepair.dfa.Alphabet;
import org.piestream.piepair.dfa.DFA;
import org.piestream.piepair.dfa.Dot2DFA;
import org.piestream.piepair.eba.EBA;
import org.piestream.merger.IEPCol;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PIEPair class handles PointEvent events, advancing the DFA state based on event classification
 * and recording the start and end points of events.
 */
public class PIEPair {
    /* Get Logger instance */
    private static final Logger logger = LoggerFactory.getLogger(PIEPair.class);

    private final DFA dfa; /* Finite State Automaton used for handling state transitions */
    private final EventClassifier classifier; /* Event classifier used to classify events into different alphabets */
    private PointEvent formerPieStart; /* Start event of the former PIE */
    private PointEvent formerPieEnd; /* End event of the former PIE */
    private PointEvent latterPieStart; /* Start event of the latter PIE */
    private PointEvent latterPieEnd; /* End event of the latter PIE */
    //    private Alphabet lastAlphabet;
//    private Alphabet currentAlphabet;
    public IEPCol Col;
    //    public IEPCol befAssistCol;
//    public IEPCol aftAssistCol;
//    private  LinkList<IE> formerIEList;
//    private  LinkList<IE> latterIEList;
    private EBA formerPred;
    private EBA latterPred;
    private TemporalRelations.PreciseRel relation;
    private boolean onTriggering; /* Current state of PIEPair, true indicates it has been triggered but not yet completed */
    private final MPIEPair mpp;
//    private final boolean isAddToCol;
//    private final boolean isAddToBefCol;
//    private final boolean isAddToAftCol;

    /**
     * Constructor that initializes PIEPair based on temporal relations and two EBA conditions.
     *
     * @param relation   The precise temporal relation used to define the temporal relationship (PreciseRel)
     * @param formerPred The former EBA
     * @param latterPred The latter EBA
     * @param mpp        The MPIEPair instance
     */
    public PIEPair(TemporalRelations.PreciseRel relation, EBA formerPred, EBA latterPred, MPIEPair mpp) {
        this.relation = relation;
        this.formerPred = formerPred;
        this.latterPred = latterPred;
        this.mpp = mpp;
        this.Col = mpp.getCol();

        this.dfa = Dot2DFA.createDFAFromRelation(relation); /* Create DFA based on the given temporal relation */
        this.classifier = new EventClassifier(formerPred, latterPred); /* Initialize event classifier using former and latter EBA */
        this.onTriggering = false;
    }

    private void tiggerEvents(PointEvent event) {
        this.onTriggering = true;
        IEP newIEP = createIEPonTrigger(event);
        logger.debug("trigger: "
                //                +newIEP.getRelation()+"["+newIEP.getFormerPie() +"," +newIEP.getLatterPie()+"] = "
                + newIEP.getRelation() + "(" + newIEP.getFormerStartTime() + "," + newIEP.getLatterStartTime() + ")");
        Col.setTriggerMSG(newIEP);
    }

    private void completeEvents() {
        this.onTriggering = false;
        updeteColWhenCompleted(Col);
    }

    /**
     * Processes an event, advances the DFA state based on the classification result,
     * and records the start and end points of the event.
     *
     * @param event          The input event (PointEvent)
     * @param newAlphabet    The new alphabet classification
     * @param formerPieStart The start event of the former PIE
     * @param formerPieEnd   The end event of the former PIE
     * @param latterPieStart The start event of the latter PIE
     * @param latterPieEnd   The end event of the latter PIE
     */
    public void stepByPE(PointEvent event, Alphabet newAlphabet, PointEvent formerPieStart,
                         PointEvent formerPieEnd, PointEvent latterPieStart, PointEvent latterPieEnd) {
        this.formerPieStart = formerPieStart;
        this.formerPieEnd = formerPieEnd;
        this.latterPieStart = latterPieStart;
        this.latterPieEnd = latterPieEnd;

        dfa.step(newAlphabet); /* Advance DFA state based on the alphabet */

        if (isTrigger()) {
            tiggerEvents(event);
        }
        if (isCompleted()) {
            completeEvents();
        }
    }

    /**
     * Determines if the DFA has reached a final state.
     *
     * @return true if the DFA has reached a final state, false otherwise
     */
    public boolean isFinal() {
        return dfa.isFinalState();
    }

    /**
     * Determines if the DFA has been triggered.
     *
     * @return true if the DFA has been triggered, false otherwise
     */
    public boolean isTrigger() {
        return dfa.isTrigger();
    }

    /**
     * Determines if the DFA has completed.
     *
     * @return true if the DFA has completed, false otherwise
     */
    public boolean isCompleted() {
        return this.onTriggering == true && (
                (relation.triggerWithoutFormerPieEnd() && mpp.isFormerPieEndTransition()) ||
                        (relation.triggerWithoutLatterPieEnd() && mpp.isLatterPieEndTransition()) ||
                        relation.triggerWithCompleted()
        );
    }

    public boolean isQUpdate() {
        return isCompleted() || isTrigger();
    }

    /**
     * Determines if the DFA's state has changed.
     *
     * @return true if the state has changed, false otherwise
     */
    public boolean isStateChanged() {
        return dfa.isStateChanged();
    }

    /* Getter methods */
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

    private IEP createIEPonTrigger(PointEvent event) {
        /* Create a new IEP object */
        IEP newIep = new IEP(
                relation,                     /* Temporal relation */
                formerPred,
                latterPred,
                formerPieStart,               /* Start event of the former PIE */
                latterPieStart,               /* Start event of the latter PIE */
                formerPieEnd,                 /* End event of the former PIE */
                latterPieEnd,                 /* End event of the latter PIE */
                formerPieStart.getTimestamp(), /* Start time of the former PIE */
                latterPieStart.getTimestamp(),  /* Start time of the latter PIE, null if latterPieStart is null */
                event,
                event.getTimestamp()
        );
        if (this.relation.triggerWithoutLatterPieEnd()) {
            newIep.setLatterPieEnd(null);
        } else if (this.relation.triggerWithoutFormerPieEnd()) {
            newIep.setFormerPieEnd(null);
        }
        return newIep;
    }

    private void updeteColWhenCompleted(IEPCol updateCol) {
        /* Determine whether to update FormerPieEnd or LatterPieEnd */
        int n = 0;  /* Tracks the number of IEPs updated */
        PointEvent currentStartEvent;

        if (this.relation.triggerWithoutLatterPieEnd()) {
            /* Find the IEP corresponding to LatterPieStart in Col's colMap */
            currentStartEvent = latterPieStart;
            Long latterStartTime = currentStartEvent.getTimestamp();
            if (latterPieEnd.getTimestamp() <= latterStartTime) {
                throw new IllegalArgumentException("latterPieEnd timestamp is earlier than latterStartTime.");
            }

            /* Retrieve the list of IEPs matching latterPred and latterStartTime from colMap */
            List<IEP> iepList = updateCol.getIEP(latterPred, latterStartTime);

            /* Update the LatterPieEnd of all found IEPs */
            for (IEP iep : iepList) {
                iep.setLatterPieEnd(latterPieEnd);
                //                iep.complete();
                n++;
            }
        } else if (this.relation.triggerWithoutFormerPieEnd()) {
            /* Find the IEP corresponding to FormerPieStart in Col's colMap */
            currentStartEvent = formerPieStart;
            Long formerStartTime = currentStartEvent.getTimestamp();
            if (formerPieEnd.getTimestamp() <= formerStartTime) {
                throw new IllegalArgumentException("latterPieEnd timestamp is earlier than latterStartTime.");
            }
            /* Retrieve the list of IEPs matching formerPred and formerStartTime from colMap */
            List<IEP> iepList = updateCol.getIEP(formerPred, formerStartTime);

            /* Update the FormerPieEnd of all found IEPs */
            for (IEP iep : iepList) {
                iep.setFormerPieEnd(formerPieEnd);
                //                iep.complete();
                n++;
            }
        } else if (this.relation.triggerWithCompleted()) {
            /* Already updated, skip directly */
            return;
        } else {
            throw new IllegalStateException("No matching IEP found to update.");
        }

        /* If n == 0, no matching IEP was found, throw an exception */
        if (n == 0) {
            throw new IllegalStateException("No matching IEP found to update.");
        }
    }

    public TemporalRelations.PreciseRel getRelation() {
        return relation;
    }

    public IEPCol getCol() {
        return Col;
    }
}
