package org.example.piepair;

import org.example.engine.MPIEPair;
import org.example.events.PointEvent;
import org.example.merger.LinkList;
import org.example.merger.TreeNode;
import org.example.piepair.dfa.Alphabet;
import org.example.piepair.dfa.DFA;
import org.example.piepair.dfa.Dot2DFA;
import org.example.piepair.eba.EBA;
import org.example.merger.IEPCol;

import java.util.List;

/**
 * PIEPair类用于处理PointEvent事件，根据事件分类推进DFA状态并记录事件的起止点。
 */
public class PIEPair {
    private final DFA dfa; // 有限状态自动机，用于处理状态转换
    private final EventClassifier classifier; // 事件分类器，用于根据事件分类为不同的字母（Alphabet）
    private PointEvent formerPieStart; // formerPie的开始事件
    private PointEvent formerPieEnd; // formerPie的结束事件
    private PointEvent latterPieStart; // latterPie的开始事件
    private PointEvent latterPieEnd; // latterPie的结束事件
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
    private boolean onTriggering ; // PIEPair 的当前状态，true表示 已经trigger但尚未complete
    private final MPIEPair mpp;
//    private final boolean isAddToCol;
//    private final boolean isAddToBefCol;
//    private final boolean isAddToAftCol;
    /**
     * 构造函数，根据时间关系和两个EBA条件初始化PIEPair。
     *
     * @param relation  用于定义时间关系的精确关系（PreciseRel）
     * @param formerPred former EBA（事件属性表达式）
     * @param latterPred latter EBA（事件属性表达式）
     */


    public PIEPair(TemporalRelations.PreciseRel relation, EBA formerPred, EBA latterPred , MPIEPair mpp) {
        this.relation=relation;
        this.formerPred=formerPred;
        this.latterPred=latterPred;
        this.mpp=mpp;
        this.Col=mpp.getCol();

        this.dfa = Dot2DFA.createDFAFromRelation(relation); // 根据给定的时间关系生成DFA
        this.classifier = new EventClassifier(formerPred, latterPred); // 使用former和latter的EBA初始化事件分类器
        this.onTriggering=false;


    }

    private void tiggerEvents(PointEvent event ){
        this.onTriggering = true;
//        System.out.println("\nTrigger! "+relation.toString()+ "\n");
        IEP newIEP =createIEPonTrigger(event);
        Col.setTriggerMSG(formerPred,newIEP.getFormerStartTime() ,latterPred, newIEP.getLatterStartTime(), newIEP);
    }


    private void completeEvents(){
        this.onTriggering = false;
//        System.out.println("\nCompleted!\n");
        updeteColWhenCompleted(Col);

    }
    /**
     * 处理事件，根据分类结果推进DFA的状态并记录事件的起止点。
     *
     * @param event 输入的事件（PointEvent）
     */
    public    void stepByPE(PointEvent event,Alphabet newAlphabet,PointEvent formerPieStart,
                                PointEvent formerPieEnd,PointEvent latterPieStart,PointEvent latterPieEnd ) {
        this.formerPieStart=formerPieStart;
        this.formerPieEnd=formerPieEnd;
        this.latterPieStart=latterPieStart;
        this.latterPieEnd=latterPieEnd;

        dfa.step(newAlphabet); // 根据字母推进DFA的状态

        if (isTrigger()) {
            tiggerEvents(event);
        }
        if (isCompleted()) {
            completeEvents();

        }


    }



    /**
     * 判断DFA是否达到了终止状态。
     *
     * @return 如果达到了终止状态则返回true，否则返回false
     */
    public boolean isFinal() {
        return dfa.isFinalState();
    }

    /**
     * 判断DFA是否被触发。
     *
     * @return 如果DFA被触发则返回true，否则返回false
     */
    public boolean isTrigger() {
        return dfa.isTrigger();
    }

    /**
     * 判断DFA是否完成。
     *
     * @return 如果DFA完成则返回true，否则返回false
     */
    public boolean isCompleted() {

        return this.onTriggering==true && (( relation.triggerWithoutFormerPieEnd() && mpp.isFormerPieEndTransition() )
                ||  relation.triggerWithoutLatterPieEnd() && mpp.isLatterPieEndTransition() || relation.triggerWithCompleted() );

    }

    public boolean isQUpdate() {

        return isCompleted()||isTrigger();
    }

    /**
     * 判断DFA的状态是否发生了变化。
     *
     * @return 如果状态发生了变化则返回true，否则返回false
     */
    public boolean isStateChanged() {
        return dfa.isStateChanged();
    }

    // getter方法
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

    private IEP createIEPonTrigger(PointEvent event){
        // 创建新的 IEP 对象
        IEP newIep = new IEP(
                relation,             // 时间关系
                formerPred,
                latterPred,
                formerPieStart,       // 前事件的开始事件
                latterPieStart,       // 后事件的开始事件
                formerPieEnd ,         // 前事件的结束事件
                latterPieEnd,         // 后事件的结束事件
                formerPieStart.getTimestamp(), // 前事件的开始时间
                latterPieStart.getTimestamp(),  // 后事件的开始时间，如果 latterPieStart 为 null，则 latterStartTime 为 null
                event,
                event.getTimestamp()
        );
        if ( this.relation.triggerWithoutLatterPieEnd()) {
            newIep.setLatterPieEnd(null);
        }
        else if ( this.relation.triggerWithoutFormerPieEnd()) {
            newIep.setFormerPieEnd(null);
        }
        return newIep;
    }

//    private void enQueueByTrigger() {
//
//        Q.enqueue(createIEPonTrigger());
////        System.out.println(newIep.toString());
//
//    }


    private void updeteColWhenCompleted(IEPCol updateCol) {
        // 确定需要更新 FormerPieEnd 还是 LatterPieEnd
        int n = 0;  // 用于跟踪更新的 IEP 数量
        PointEvent currentStartEvent;

        if (this.relation.triggerWithoutLatterPieEnd()) {
            // 从 Col 的 colMap 中查找 LatterPieStart 对应的 IEP
            currentStartEvent = latterPieStart;
            Long latterStartTime = currentStartEvent.getTimestamp();
            if (latterPieEnd.getTimestamp() <= latterStartTime) {
                throw new IllegalArgumentException("latterPieEnd timestamp is earlier than latterStartTime.");
            }

            // 从 colMap 中获取与 latterPred 和 latterStartTime 匹配的 IEP 列表
            List<IEP> iepList = updateCol.getIEP(latterPred, latterStartTime);

            // 更新所有找到的 IEP 的 LatterPieEnd
            for (IEP iep : iepList) {
                iep.setLatterPieEnd(latterPieEnd);
//                iep.complete();
                n++;

            }
        } else if (this.relation.triggerWithoutFormerPieEnd()) {
            // 从 Col 的 colMap 中查找 FormerPieStart 对应的 IEP
            currentStartEvent = formerPieStart;
            Long formerStartTime = currentStartEvent.getTimestamp();
            if (formerPieEnd.getTimestamp() <= formerStartTime) {
                throw new IllegalArgumentException("latterPieEnd timestamp is earlier than latterStartTime.");
            }
            // 从 colMap 中获取与 formerPred 和 formerStartTime 匹配的 IEP 列表
            List<IEP> iepList = updateCol.getIEP(formerPred, formerStartTime);


            // 更新所有找到的 IEP 的 FormerPieEnd
            for (IEP iep : iepList) {
                iep.setFormerPieEnd(formerPieEnd);
//                iep.complete();
                n++;

            }
        }else if( this.relation.triggerWithCompleted() ){
            // 已经更新好，直接跳过
            return ;

        }else{
            throw new IllegalStateException("No matching IEP found to update.");
        }

        // 如果 n == 0，表示没有找到匹配的 IEP，抛出异常
        if (n == 0) {
            throw new IllegalStateException("No matching IEP found to update.");
        }
    }



    public TemporalRelations.PreciseRel getRelation(){
        return relation;
    }

    public IEPCol getCol(){
        return Col;
    }

}
