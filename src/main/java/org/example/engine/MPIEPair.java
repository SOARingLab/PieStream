package org.example.engine;

import org.example.piepair.IEP;
import org.example.piepair.PIEPair;
import org.example.piepair.eba.EBA;
import org.example.piepair.TemporalRelations;
import org.example.utils.CircularQueue;

import java.util.ArrayList;
import java.util.List;

public class MPIEPair {
    private final List<TemporalRelations.PreciseRel> relations;  // 时间关系的精确关系列表
    private final EBA formerPred;  // former EBA（事件属性表达式）
    private final EBA latterPred;  // latter EBA（事件属性表达式）
    private final int QCapacity;  // CircularQueue 的容量
    private final List<PIEPair> piePairs;  // PIEPair 的列表
    private final List<CircularQueue<IEP>> QList;  // CircularQueue 对象
    /**
     * 构造函数，初始化 MPIEPair，并根据传入的 PreciseRel 列表创建多个 PIEPair。
     *
     * @param relations  时间关系的精确关系列表（PreciseRel 列表）
     * @param formerPred former EBA（事件属性表达式）
     * @param latterPred latter EBA（事件属性表达式）
     * @param QCapacity  CircularQueue 的容量
     */
    public MPIEPair(List<TemporalRelations.PreciseRel> relations, EBA formerPred, EBA latterPred, int QCapacity) {
        this.relations = relations;
        this.formerPred = formerPred;
        this.latterPred = latterPred;
        this.QCapacity = QCapacity;
        this.piePairs = new ArrayList<>();
        this.QList= new ArrayList<>();

        for (TemporalRelations.PreciseRel relation : relations) {
            PIEPair pp=new PIEPair(relation, formerPred, latterPred, QCapacity);
            this.piePairs.add(pp);
            this.QList.add(pp.getQ());
        }
    }

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

    /**
     * 获取 CircularQueue 对象。
     *
     * @return CircularQueue 对象
     */
    public List<CircularQueue<IEP>> getQList() {
        return QList;
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
