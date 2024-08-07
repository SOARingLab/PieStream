package org.example.piepair;

public class TemporalRelations {
    public enum AllRel {
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
        MET_BY,
        FOLLOWED_BY,
        FOLLOW
    }

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

    public enum PreciseRel {
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
        MET_BY,
        FOLLOWED_BY,
        FOLLOW
    }
}
