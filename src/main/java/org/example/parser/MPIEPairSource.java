package org.example.parser;

import org.example.piepair.IEP;
import org.example.piepair.PIEPair;
import org.example.piepair.TemporalRelations;
import org.example.piepair.eba.EBA;
import org.example.utils.CircularQueue;

import java.util.ArrayList;
import java.util.List;

public class MPIEPairSource {
    private final List<TemporalRelations.PreciseRel> relations;  // 时间关系的精确关系列表
    private final EBA formerPred;  // former EBA（事件属性表达式）
    private final EBA latterPred;  // latter EBA（事件属性表达式）

    public MPIEPairSource(List<TemporalRelations.PreciseRel> relations, EBA formerPred, EBA latterPred) {
        this.relations = relations;
        this.formerPred = formerPred;
        this.latterPred = latterPred;
    }

    // 获取 relations
    public List<TemporalRelations.PreciseRel> getRelations() {
        return relations;
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
