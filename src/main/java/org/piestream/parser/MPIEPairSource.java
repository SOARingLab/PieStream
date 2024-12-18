package org.piestream.parser;

import org.piestream.piepair.TemporalRelations;
import org.piestream.piepair.eba.EBA;

import java.util.*;

import static org.piestream.piepair.TemporalRelations.AllRel.BEFORE;
import static org.piestream.piepair.TemporalRelations.AllRel.AFTER;
import static org.piestream.piepair.TemporalRelations.PreciseRel.*;

/**
 * This class represents a source of MPIE pairs, which involves the relationships
 * between two event attributes, defined as the "former" and "latter" predicates.
 * It handles the temporal relations between the events and stores the precise relations
 * for both "before" and "after" events.
 */
public class MPIEPairSource {
    private final Set<TemporalRelations.PreciseRel> relations = new HashSet<>(); // List of precise temporal relations
    private final Set<TemporalRelations.PreciseRel> originRels = new HashSet<>(); // Original set of precise temporal relations
    private final EBA formerPred; // The "former" event predicate (EBA)
    private final EBA latterPred; // The "latter" event predicate (EBA)
    private boolean hasBeforeRel = false; // Flag to indicate if "before" relation exists
    private boolean hasAfterRel = false; // Flag to indicate if "after" relation exists
    // private final Set<TemporalRelations.PreciseRel> relSetBefore=new HashSet<>(
    // Arrays.asList(FOLLOWED_BY,MEETS,OVERLAPS,STARTS,DURING,FINISHES,FINISHED_BY,EQUALS));
    // private final Set<TemporalRelations.PreciseRel> relSetAfter=new HashSet<>(
    // Arrays.asList(FOLLOW,MET_BY,
    // OVERLAPPED_BY,DURING,FINISHES,STARTS,STARTED_BY,EQUALS));

    /**
     * Constructor to initialize the MPIEPairSource with the given temporal relations
     * and the former and latter event predicates.
     *
     * @param rels A list of temporal relations (before, after, or precise)
     * @param formerPred The former event predicate (EBA)
     * @param latterPred The latter event predicate (EBA)
     */
    public MPIEPairSource(List<TemporalRelations.AllRel> rels, EBA formerPred, EBA latterPred) {
        this.formerPred = formerPred;
        this.latterPred = latterPred;

        // Iterate over the provided relations and categorize them into before, after, or precise relations
        for (TemporalRelations.AllRel rel : rels) {
            if (rel == BEFORE) {
                this.hasBeforeRel = true; // Mark the presence of "before" relation
                this.relations.add(FOLLOWED_BY); // Add FOLLOWED_BY relation
                this.relations.addAll(TemporalRelations.getRelSetBefore()); // Add all relations from the set of relations before
            } else if (rel == AFTER) {
                this.hasAfterRel = true; // Mark the presence of "after" relation
                this.relations.add(FOLLOW); // Add FOLLOW relation
                this.relations.addAll(TemporalRelations.getRelSetAfter()); // Add all relations from the set of relations after
            } else {
                originRels.add(rel.getPreciseRel()); // Add precise relations to the origin relations set
            }
        }
        this.relations.addAll(originRels); // Add all original relations to the relations set
    }

    /**
     * Checks if there is an "after" relation.
     *
     * @return true if the "after" relation exists, false otherwise
     */
    public boolean isHasAfterRel() {
        return hasAfterRel;
    }

    /**
     * Checks if there is a "before" relation.
     *
     * @return true if the "before" relation exists, false otherwise
     */
    public boolean isHasBeforeRel() {
        return hasBeforeRel;
    }

    /**
     * Retrieves the set of all temporal relations associated with this MPIE pair source.
     *
     * @return A set of precise temporal relations
     */
    public Set<TemporalRelations.PreciseRel> getRelations() {
        return relations;
    }

    /**
     * Retrieves the original set of precise temporal relations.
     *
     * @return A set of original precise temporal relations
     */
    public Set<TemporalRelations.PreciseRel> getOriginRelations() {
        return originRels;
    }

    /**
     * Retrieves the former event predicate (EBA).
     *
     * @return The former event predicate (EBA)
     */
    public EBA getFormerPred() {
        return formerPred;
    }

    /**
     * Retrieves the latter event predicate (EBA).
     *
     * @return The latter event predicate (EBA)
     */
    public EBA getLatterPred() {
        return latterPred;
    }
}
