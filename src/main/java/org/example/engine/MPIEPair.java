package org.example.engine;

import org.example.events.PointEvent;
import org.example.merger.LinkList;
import org.example.merger.TreeNode;
import org.example.piepair.*;
import org.example.piepair.dfa.Alphabet;
import org.example.piepair.eba.EBA;
import org.example.merger.IEPCol;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MPIEPair {
    private final Set<TemporalRelations.PreciseRel> relations;  // 时间关系的精确关系列表
    private final EventClassifier classifier;                      // 事件分类器
    private final EBA formerPred;                                  // 前EBA
    private final EBA latterPred;                                  // 后EBA
    private final int QCapacity;                                   // 队列容量
    private final List<PIEPair> piePairs;                         // PIEPair 列表
    private Alphabet lastAlphabet;                                  // 上一个字母
    private Alphabet currentAlphabet;                               // 当前字母
    private PointEvent formerPieStart;                             // 前PIE开始事件
    private PointEvent formerPieEnd;                               // 前PIE结束事件
    private PointEvent latterPieStart;                             // 后PIE开始事件
    private PointEvent latterPieEnd;                               // 后PIE结束事件
    private LinkList<IE> formerIEList;                            // 前事件列表
    private LinkList<IE> latterIEList;                            // 后事件列表
    private final TreeNode node;                                   // 树节点
    private final boolean hasBefore;                               // 是否有before关系
    private final boolean hasAfter;                                // 是否有after关系
    private boolean hasNewFormerIE;                                // 是否有新前事件
    private boolean hasNewLatterIE;                                // 是否有新后事件

    /**
     * 构造函数，初始化 MPIEPair 并创建多个 PIEPair。
     *
     * @param relations  精确关系列表
     * @param formerPred 前EBA
     * @param latterPred 后EBA
     * @param node      树节点
     */
    public MPIEPair(Set<TemporalRelations.PreciseRel> relations, EBA formerPred, EBA latterPred, TreeNode node) {
        if (relations == null || formerPred == null || latterPred == null) {
            throw new IllegalArgumentException("参数不能为空");
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

    public PointEvent getFormerPieStart(){
        return formerPieStart;
    }

    public PointEvent getFormerPieEnd() {
        return formerPieEnd;
    }

    public PointEvent getLatterPieStart() {
        return latterPieStart;
    }

    public PointEvent getLatterPieEnd(){
        return latterPieEnd;
    }

    private void resetNewIE() {
        hasNewFormerIE = false;
        hasNewLatterIE = false;
    }

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
     * 获取时间关系的精确关系列表。
     *
     * @return 精确关系列表
     */
    public Set<TemporalRelations.PreciseRel> getRelations() {
        return relations;
    }

    /**
     * 获取前EBA。
     *
     * @return 前EBA
     */
    public EBA getFormerPred() {
        return formerPred;
    }

    /**
     * 获取后EBA。
     *
     * @return 后EBA
     */
    public EBA getLatterPred() {
        return latterPred;
    }

    /**
     * 获取队列容量。
     *
     * @return 队列容量
     */
    public int getQCapacity() {
        return QCapacity;
    }

    public IEPCol getCol() {
        return node.getCol();
    }

    private void recordIntervalEvent(PointEvent event) {
        if (isFormerPieStartTransition()) {
            formerPieStart = event;
            formerPieEnd = null;
            hasNewFormerIE = true;
        }
        if (isFormerPieEndTransition()) {
            formerPieEnd = event;
            if (hasBefore) {
                formerIEList.add(new IE(formerPred, formerPieStart, formerPieEnd));
            }
            if (hasAfter) {
                node.getAftCol().updateCompletedMSG("former",formerPred,formerPieStart.getTimestamp(),formerPieEnd);
//                node.getAftCol().printCol();
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
                latterIEList.add(new IE(latterPred, latterPieStart, latterPieEnd));
            }
            if (hasBefore) {

                node.getBefCol().updateCompletedMSG("latter",latterPred,latterPieStart.getTimestamp(),latterPieEnd);
//                node.getBefCol().printCol();
            }
        }
    }

    /**
     * 判断是否为前PIE开始转换。
     *
     * @return 如果是开始转换则返回true
     */
    public boolean isFormerPieStartTransition() {
        return (lastAlphabet == Alphabet.O || lastAlphabet == Alphabet.I || lastAlphabet == null) &&
                (currentAlphabet == Alphabet.Z || currentAlphabet == Alphabet.E);
    }

    /**
     * 判断是否为前PIE结束转换。
     *
     * @return 如果是结束转换则返回true
     */
    public boolean isFormerPieEndTransition() {
        return (lastAlphabet == Alphabet.Z || lastAlphabet == Alphabet.E) &&
                (currentAlphabet == Alphabet.O || currentAlphabet == Alphabet.I);
    }

    /**
     * 判断是否为后PIE开始转换。
     *
     * @return 如果是开始转换则返回true
     */
    public boolean isLatterPieStartTransition() {
        return (lastAlphabet == Alphabet.O || lastAlphabet == Alphabet.Z || lastAlphabet == null) &&
                (currentAlphabet == Alphabet.I || currentAlphabet == Alphabet.E);
    }

    /**
     * 判断是否为后PIE结束转换。
     *
     * @return 如果是结束转换则返回true
     */
    public boolean isLatterPieEndTransition() {
        return (lastAlphabet == Alphabet.I || lastAlphabet == Alphabet.E) &&
                (currentAlphabet == Alphabet.O || currentAlphabet == Alphabet.Z);
    }

    /**
     * 获取所有 PIEPair 的列表。
     *
     * @return 所有 PIEPair 列表
     */
    public List<PIEPair> getPiePairs() {
        return piePairs;
    }

//
//    private void updeteColWhenCompleted(IEPCol updateCol) {
//        // 确定需要更新 FormerPieEnd 还是 LatterPieEnd
//        int n = 0;  // 用于跟踪更新的 IEP 数量
//        PointEvent currentStartEvent;
//
//        if (this.relation.triggerWithoutLatterPieEnd()) {
//            // 从 Col 的 colMap 中查找 LatterPieStart 对应的 IEP
//            currentStartEvent = latterPieStart;
//            Long latterStartTime = currentStartEvent.getTimestamp();
//            if (latterPieEnd.getTimestamp() <= latterStartTime) {
//                throw new IllegalArgumentException("latterPieEnd timestamp is earlier than latterStartTime.");
//            }
//
//            // 从 colMap 中获取与 latterPred 和 latterStartTime 匹配的 IEP 列表
//            List<IEP> iepList = updateCol.getIEP(latterPred, latterStartTime);
//
//            // 更新所有找到的 IEP 的 LatterPieEnd
//            for (IEP iep : iepList) {
//                iep.setLatterPieEnd(latterPieEnd);
////                iep.complete();
//                n++;
//
//            }
//        } else if (this.relation.triggerWithoutFormerPieEnd()) {
//            // 从 Col 的 colMap 中查找 FormerPieStart 对应的 IEP
//            currentStartEvent = formerPieStart;
//            Long formerStartTime = currentStartEvent.getTimestamp();
//            if (formerPieEnd.getTimestamp() <= formerStartTime) {
//                throw new IllegalArgumentException("latterPieEnd timestamp is earlier than latterStartTime.");
//            }
//            // 从 colMap 中获取与 formerPred 和 formerStartTime 匹配的 IEP 列表
//            List<IEP> iepList = updateCol.getIEP(formerPred, formerStartTime);
//
//
//            // 更新所有找到的 IEP 的 FormerPieEnd
//            for (IEP iep : iepList) {
//                iep.setFormerPieEnd(formerPieEnd);
////                iep.complete();
//                n++;
//
//            }
//        }else if( this.relation.triggerWithCompleted() ){
//            // 已经更新好，直接跳过
//            return ;
//
//        }else{
//            throw new IllegalStateException("No matching IEP found to update.");
//        }
//
//        // 如果 n == 0，表示没有找到匹配的 IEP，抛出异常
//        if (n == 0) {
//            throw new IllegalStateException("No matching IEP found to update.");
//        }
//    }


    @Override
    public String toString() {
        return "MPIEPair{" +
                "relations=" + relations +
                ", formerPred=" + formerPred +
                ", latterPred=" + latterPred +
                '}';
    }
}
