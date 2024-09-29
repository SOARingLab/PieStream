package org.example.parser;

import org.example.piepair.TemporalRelations;
import org.example.piepair.eba.EBA;
import sun.plugin2.os.windows.OVERLAPPED;

import java.util.*;

import static org.example.piepair.TemporalRelations.AllRel.BEFORE;
import static org.example.piepair.TemporalRelations.AllRel.AFTER;
import static org.example.piepair.TemporalRelations.PreciseRel.*;

public class MPIEPairSource {
    private final Set<TemporalRelations.PreciseRel> relations =new HashSet<>();  // 时间关系的精确关系列表
    private final Set<TemporalRelations.PreciseRel> originRels =new HashSet<>();  // 时间关系的精确关系列表
    private final EBA formerPred;  // former EBA（事件属性表达式）
    private final EBA latterPred;  // latter EBA（事件属性表达式）
    private   boolean hasBeforeRel =false;  // latter EBA（事件属性表达式）
    private   boolean hasAfterRel=false;
//    private final Set<TemporalRelations.PreciseRel> relSetBefore=new HashSet<>( Arrays.asList(FOLLOWED_BY,MEETS,OVERLAPS,STARTS,DURING,FINISHES,FINISHED_BY,EQUALS));
//    private final Set<TemporalRelations.PreciseRel> relSetAfter=new HashSet<>( Arrays.asList(FOLLOW,MET_BY, OVERLAPPED_BY,DURING,FINISHES,STARTS,STARTED_BY,EQUALS));

    public MPIEPairSource(List<TemporalRelations.AllRel> rels, EBA formerPred, EBA latterPred) {
        this.formerPred = formerPred;
        this.latterPred = latterPred;


        for (TemporalRelations.AllRel rel :rels){
            if (rel== BEFORE){
                this.hasBeforeRel=true;
                this.relations.add (FOLLOWED_BY);
                this.relations.addAll(TemporalRelations.getRelSetBefore() );
            }
            else if (rel== AFTER){
                this.hasAfterRel=true;
                this.relations.add (FOLLOW);
                this.relations.addAll(TemporalRelations.getRelSetAfter());
            }
            else {
                originRels.add(rel.getPreciseRel());
            }
        }
        this.relations.addAll(originRels);
    }

    public boolean isHasAfterRel() {
        return hasAfterRel;
    }

    public boolean isHasBeforeRel() {
        return hasBeforeRel;
    }

    // 获取 relations
    public Set<TemporalRelations.PreciseRel> getRelations() {
        return relations;
    }
    public Set<TemporalRelations.PreciseRel> getOriginRelations() {
        return originRels;
    }
    // 获取 formerPred
    public EBA getFormerPred() {
        return formerPred;
    }

    // 获取 latterPred
    public EBA getLatterPred() {
        return latterPred;
    }
}
