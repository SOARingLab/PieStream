package org.example.engine;

import org.example.piepair.IEP;
import org.example.piepair.PIEPair;
import org.example.piepair.eba.EBA;
import org.example.piepair.TemporalRelations;
import org.example.utils.CircularQueue;
import org.example.merger.IEPQ;
import org.example.utils.IEPCol;
import org.example.utils.LinkList;

import java.util.ArrayList;
import java.util.List;

public class MPIEPair {
    private final List<TemporalRelations.PreciseRel> relations;  // 时间关系的精确关系列表
    private final EBA formerPred;  // former EBA（事件属性表达式）
    private final EBA latterPred;  // latter EBA（事件属性表达式）
    private final int QCapacity;  // CircularQueue 的容量
    private final List<PIEPair> piePairs;  // PIEPair 的列表
//    private final List<CircularQueue<IEP>> QList;  // CircularQueue 对象
//    private final  IEPQ  sharedQ;  // CircularQueue 对象
//    private final boolean isShared;
//    private IEPUpdateStruct M;

    private final IEPCol Col;

    /**
     * 构造函数，初始化 MPIEPair，并根据传入的 PreciseRel 列表创建多个 PIEPair。
     *
     * @param relations  时间关系的精确关系列表（PreciseRel 列表）
     * @param formerPred former EBA（事件属性表达式）
     * @param latterPred latter EBA（事件属性表达式）
     */
//    public MPIEPair(List<TemporalRelations.PreciseRel> relations, EBA formerPred, EBA latterPred, int queueCapacity, boolean isShared) {
//        if (relations == null || formerPred == null || latterPred == null) {
//            throw new IllegalArgumentException("参数不能为空");
//        }
//        this.relations = new ArrayList<>(relations);
//        this.formerPred = formerPred;
//        this.latterPred = latterPred;
//        this.QCapacity = queueCapacity;
//        this.piePairs = new ArrayList<>();
//        this.isShared = isShared;
//
//        if (isShared) {
//            this.QList = null;
//            this.sharedQ = new IEPQ(queueCapacity,formerPred,latterPred);
//            initializePIEPairsWithSharedQueue();
//        } else {
//            this.QList = new ArrayList<>();
//            this.sharedQ = null;
//            initializePIEPairsWithSeparateQueues();
//        }
//    }

    public MPIEPair(List<TemporalRelations.PreciseRel> relations, EBA formerPred, EBA latterPred, IEPCol Col) {
        if (relations == null || formerPred == null || latterPred == null) {
            throw new IllegalArgumentException("参数不能为空");
        }
        this.relations = new ArrayList<>(relations);
        this.formerPred = formerPred;
        this.latterPred = latterPred;
        this.QCapacity = 0;
        this.piePairs = new ArrayList<>();
        this.Col=Col;
        for (TemporalRelations.PreciseRel relation : relations) {
            PIEPair pp = new PIEPair(relation, formerPred, latterPred,Col);
            this.piePairs.add(pp);
        }
    }
//
//    private void initializePIEPairsWithSharedQueue() {
//        for (TemporalRelations.PreciseRel relation : relations) {
//            PIEPair pp = new PIEPair(relation, formerPred, latterPred, sharedQ.getQ());
//            this.piePairs.add(pp);
//        }
//    }
//
//    private void initializePIEPairsWithSeparateQueues() {
//        for (TemporalRelations.PreciseRel relation : relations) {
//            PIEPair pp = new PIEPair(relation, formerPred, latterPred, QCapacity);
//            this.piePairs.add(pp);
//            this.QList.add(pp.getQ());
//        }
//    }

    /**
     * 获取时间关系的精确关系列表。
     *
     * @return 时间关系的精确关系列表
     */
    public List<TemporalRelations.PreciseRel> getRelations() {
        return relations;
    }

    /**
     * 获取 former EBA（事件属性表达式）。
     *
     * @return former EBA
     */
    public EBA getFormerPred() {
        return formerPred;
    }

    /**
     * 获取 latter EBA（事件属性表达式）。
     *
     * @return latter EBA
     */
    public EBA getLatterPred() {
        return latterPred;
    }

    /**
     * 获取 CircularQueue 的容量。
     *
     * @return CircularQueue 的容量
     */
    public int getQCapacity() {
        return QCapacity;
    }

    public IEPCol getCol() {
        return Col;
    }


    /**
     * 获取所有 PIEPair 的列表。
     *
     * @return 所有 PIEPair 的列表
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
