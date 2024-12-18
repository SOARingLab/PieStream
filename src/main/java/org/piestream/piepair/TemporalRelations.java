package org.piestream.piepair;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The TemporalRelations class defines various temporal relationships between events using Allen's interval algebra.
 * It includes enums for different types of temporal relations, methods to retrieve specific sets of relations,
 * and utilities to convert between different relation types.
 */
public class TemporalRelations {

    /**
     * Enum representing Allen's basic temporal relations between two intervals.
     */
    public enum AllenRel {
        BEFORE,
        AFTER,
        OVERLAPS,
        OVERLAPPED_BY,
        STARTS,
        STARTED_BY,
        DURING,
        CONTAINS,
        FINISHES,
        FINISHED_BY,
        EQUALS,
        MEETS,
        MET_BY
    }

    /**
     * Enum representing more precise temporal relations with additional triggering conditions.
     */
    public enum PreciseRel {
        OVERLAPS(true, false, false),
        OVERLAPPED_BY(false, true, false),
        STARTS(true, false, false),
        STARTED_BY(false, true, false),
        DURING(true, false, false),
        CONTAINS(false, true, false),
        FINISHES(false, false, true),
        FINISHED_BY(false, false, true),
        EQUALS(false, false, true),
        MEETS(true, false, false),
        MET_BY(false, true, false),
        FOLLOWED_BY(true, false, false),
        FOLLOW(false, true, false);

        private final boolean triggerWithoutLatterPieEnd;
        private final boolean triggerWithoutFormerPieEnd;
        private final boolean triggerWithCompleted;

        /**
         * Constructs a PreciseRel with specified triggering conditions.
         *
         * @param triggerWithoutLatterPieEnd Indicates if the relation triggers without the end of the latter PIE
         * @param triggerWithoutFormerPieEnd Indicates if the relation triggers without the end of the former PIE
         * @param triggerWithCompleted       Indicates if the relation triggers when completed
         */
        PreciseRel(boolean triggerWithoutLatterPieEnd, boolean triggerWithoutFormerPieEnd, boolean triggerWithCompleted) {
            this.triggerWithoutLatterPieEnd = triggerWithoutLatterPieEnd;
            this.triggerWithoutFormerPieEnd = triggerWithoutFormerPieEnd;
            this.triggerWithCompleted = triggerWithCompleted;
        }

        /**
         * Checks if the relation triggers without the end of the latter PIE.
         *
         * @return true if it triggers without the end of the latter PIE, false otherwise
         */
        public boolean triggerWithoutLatterPieEnd() {
            return triggerWithoutLatterPieEnd;
        }

        /**
         * Checks if the relation triggers without the end of the former PIE.
         *
         * @return true if it triggers without the end of the former PIE, false otherwise
         */
        public boolean triggerWithoutFormerPieEnd() {
            return triggerWithoutFormerPieEnd;
        }

        /**
         * Checks if the relation triggers when completed.
         *
         * @return true if it triggers when completed, false otherwise
         */
        public boolean triggerWithCompleted() {
            return triggerWithCompleted;
        }
    }

    /**
     * Retrieves a set of PreciseRel representing relations where the former event ends before or meets the latter event.
     *
     * @return A set of PreciseRel that occur before the latter event
     */
    public static Set<PreciseRel> getRelSetBefore() {
        return new HashSet<>(Arrays.asList(
                // PreciseRel.FOLLOWED_BY,
                PreciseRel.MEETS,
                PreciseRel.OVERLAPS,
                PreciseRel.STARTS,
                PreciseRel.DURING,
                PreciseRel.FINISHES,
                PreciseRel.FINISHED_BY,
                PreciseRel.EQUALS
        ));
    }

    /**
     * Retrieves a set of PreciseRel representing relations where the latter event starts after or is met by the former event.
     *
     * @return A set of PreciseRel that occur after the former event
     */
    public static Set<PreciseRel> getRelSetAfter() {
        return new HashSet<>(Arrays.asList(
                // PreciseRel.FOLLOW,
                PreciseRel.MET_BY,
                PreciseRel.OVERLAPPED_BY,
                PreciseRel.DURING,
                PreciseRel.FINISHES,
                PreciseRel.STARTS,
                PreciseRel.STARTED_BY,
                PreciseRel.EQUALS
        ));
    }

    /**
     * Enum representing all possible temporal relations, combining AllenRel and PreciseRel where applicable.
     */
    public enum AllRel {
        BEFORE(AllenRel.BEFORE),
        AFTER(AllenRel.AFTER),
        OVERLAPS(AllenRel.OVERLAPS, PreciseRel.OVERLAPS),
        OVERLAPPED_BY(AllenRel.OVERLAPPED_BY, PreciseRel.OVERLAPPED_BY),
        STARTS(AllenRel.STARTS, PreciseRel.STARTS),
        STARTED_BY(AllenRel.STARTED_BY, PreciseRel.STARTED_BY),
        DURING(AllenRel.DURING, PreciseRel.DURING),
        CONTAINS(AllenRel.CONTAINS, PreciseRel.CONTAINS),
        FINISHES(AllenRel.FINISHES, PreciseRel.FINISHES),
        FINISHED_BY(AllenRel.FINISHED_BY, PreciseRel.FINISHED_BY),
        EQUALS(AllenRel.EQUALS, PreciseRel.EQUALS),
        MEETS(AllenRel.MEETS, PreciseRel.MEETS),
        MET_BY(AllenRel.MET_BY, PreciseRel.MET_BY),
        FOLLOWED_BY(PreciseRel.FOLLOWED_BY),
        FOLLOW(PreciseRel.FOLLOW);

        private final AllenRel allenRel;
        private final PreciseRel preciseRel;

        /**
         * Constructs an AllRel with only an AllenRel.
         *
         * @param allenRel The Allen temporal relation
         */
        AllRel(AllenRel allenRel) {
            this.allenRel = allenRel;
            this.preciseRel = null;
        }

        /**
         * Constructs an AllRel with only a PreciseRel.
         *
         * @param preciseRel The precise temporal relation
         */
        AllRel(PreciseRel preciseRel) {
            this.allenRel = null;
            this.preciseRel = preciseRel;
        }

        /**
         * Constructs an AllRel with both an AllenRel and a PreciseRel.
         *
         * @param allenRel   The Allen temporal relation
         * @param preciseRel The precise temporal relation
         */
        AllRel(AllenRel allenRel, PreciseRel preciseRel) {
            this.allenRel = allenRel;
            this.preciseRel = preciseRel;
        }

        /**
         * Checks if this AllRel includes an AllenRel.
         *
         * @return true if it includes an AllenRel, false otherwise
         */
        public boolean isAllenRel() {
            return this.allenRel != null;
        }

        /**
         * Checks if this AllRel includes a PreciseRel.
         *
         * @return true if it includes a PreciseRel, false otherwise
         */
        public boolean isPreciseRel() {
            return this.preciseRel != null;
        }

        /**
         * Retrieves the associated AllenRel.
         *
         * @return The AllenRel if present, otherwise null
         */
        public AllenRel getAllenRel() {
            return allenRel;
        }

        /**
         * Retrieves the associated PreciseRel.
         *
         * @return The PreciseRel if present, otherwise null
         */
        public PreciseRel getPreciseRel() {
            return preciseRel;
        }

        /**
         * Converts a PreciseRel to its corresponding AllRel.
         *
         * @param preciseRel The PreciseRel to convert
         * @return The corresponding AllRel
         * @throws IllegalArgumentException if no matching AllRel is found
         */
        public static AllRel fromPreciseRel(PreciseRel preciseRel) {
            for (AllRel rel : AllRel.values()) {
                if (rel.preciseRel == preciseRel) {
                    return rel;
                }
            }
            throw new IllegalArgumentException("No matching AllRel for PreciseRel: " + preciseRel);
        }

        /**
         * Converts an AllenRel to its corresponding AllRel.
         *
         * @param allenRel The AllenRel to convert
         * @return The corresponding AllRel
         * @throws IllegalArgumentException if no matching AllRel is found
         */
        public static AllRel fromAllenRel(AllenRel allenRel) {
            for (AllRel rel : AllRel.values()) {
                if (rel.allenRel == allenRel) {
                    return rel;
                }
            }
            throw new IllegalArgumentException("No matching AllRel for AllenRel: " + allenRel);
        }

        /**
         * Converts a string to its corresponding AllRel.
         *
         * @param relStr The string representation of the relation
         * @return The corresponding AllRel
         * @throws IllegalArgumentException if no matching AllRel is found
         */
        public static AllRel fromString(String relStr) {
            // Normalize the string (convert to uppercase and replace spaces with underscores)
            String normalizedRelStr = relStr.toUpperCase().replace(" ", "").replace("-", "_");
            for (AllRel rel : AllRel.values()) {
                if (rel.name().equals(normalizedRelStr)) {
                    return rel;
                }
            }
            throw new IllegalArgumentException("No matching AllRel for string: " + relStr);
        }
    }
}
