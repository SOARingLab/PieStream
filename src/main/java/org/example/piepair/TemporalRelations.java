package org.example.piepair;

public class TemporalRelations {

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
        OVERLAPS(true, false,false),
        OVERLAPPED_BY(false, true,false),
        STARTS(true, false,false),
        STARTED_BY(false, true,false),
        DURING(true, false,false),
        CONTAINS(false, true,false),
        FINISHES(false, false,true),
        FINISHED_BY(false, false,true),
        EQUALS(false, false,true),
        MEETS(true, false,false),
        MET_BY(false, true,false),
        FOLLOWED_BY(true, false,false),
        FOLLOW(false, true,false);

        private final boolean triggerWithoutLatterPieEnd;
        private final boolean triggerWithoutFormerPieEnd;
        private final boolean triggerWithCompleted;
        // 构造函数
        PreciseRel(boolean triggerWithoutLatterPieEnd, boolean triggerWithoutFormerPieEnd,boolean triggerWithCompleted) {
            this.triggerWithoutLatterPieEnd = triggerWithoutLatterPieEnd;
            this.triggerWithoutFormerPieEnd = triggerWithoutFormerPieEnd;
            this.triggerWithCompleted=triggerWithCompleted;
        }

        // 判断是否需要在没有 LatterPieEnd 的情况下触发
        public boolean triggerWithoutLatterPieEnd() {
            return triggerWithoutLatterPieEnd;
        }

        // 判断是否需要在没有 FormerPieEnd 的情况下触发
        public boolean triggerWithoutFormerPieEnd() {
            return triggerWithoutFormerPieEnd;
        }

        // 判断是否需要在没有 FormerPieEnd 的情况下触发
        public boolean triggerWithCompleted() {
            return triggerWithCompleted;
        }
    }

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

        // 构造函数
        AllRel(AllenRel allenRel) {
            this.allenRel = allenRel;
            this.preciseRel = null;
        }

        AllRel(PreciseRel preciseRel) {
            this.allenRel = null;
            this.preciseRel = preciseRel;
        }

        AllRel(AllenRel allenRel, PreciseRel preciseRel) {
            this.allenRel = allenRel;
            this.preciseRel = preciseRel;
        }


        // 判断 AllRel 是否包含 AllenRel
        public boolean isAllenRel() {
            return this.allenRel != null;
        }

        // 判断 AllRel 是否包含 PreciseRel
        public boolean isPreciseRel() {
            return this.preciseRel != null;
        }

        // 获取关联的 AllenRel
        public AllenRel getAllenRel() {
            return allenRel;
        }

        // 获取关联的 PreciseRel
        public PreciseRel getPreciseRel() {
            return preciseRel;
        }

        // 根据 PreciseRel 查找对应的 AllRel
        public static AllRel fromPreciseRel(PreciseRel preciseRel) {
            for (AllRel rel : AllRel.values()) {
                if (rel.preciseRel == preciseRel) {
                    return rel;
                }
            }
            throw new IllegalArgumentException("No matching AllRel for PreciseRel: " + preciseRel);
        }

        // 根据 AllenRel 查找对应的 AllRel
        public static AllRel fromAllenRel(AllenRel allenRel) {
            for (AllRel rel : AllRel.values()) {
                if (rel.allenRel == allenRel) {
                    return rel;
                }
            }
            throw new IllegalArgumentException("No matching AllRel for AllenRel: " + allenRel);
        }
        // 根据字符串构造 AllRel
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
