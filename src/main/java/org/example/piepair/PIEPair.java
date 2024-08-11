package org.example.piepair;

import org.example.events.PointEvent;
import org.example.piepair.dfa.Alphabet;
import org.example.piepair.dfa.DFA;
import org.example.piepair.dfa.Dot2DFA;
import org.example.piepair.eba.EBA;

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
    private Alphabet lastAlphabet;
    private Alphabet currentAlphabet;

    /**
     * 构造函数，根据时间关系和两个EBA条件初始化PIEPair。
     *
     * @param relation  用于定义时间关系的精确关系（PreciseRel）
     * @param formerPred former EBA（事件属性表达式）
     * @param latterPred latter EBA（事件属性表达式）
     */
    public PIEPair(TemporalRelations.PreciseRel relation, EBA formerPred, EBA latterPred) {
        this.dfa = Dot2DFA.createDFAFromRelation(relation); // 根据给定的时间关系生成DFA
        this.classifier = new EventClassifier(formerPred, latterPred); // 使用former和latter的EBA初始化事件分类器
    }

    /**
     * 处理事件，根据分类结果推进DFA的状态并记录事件的起止点。
     *
     * @param event 输入的事件（PointEvent）
     */
    public Alphabet stepByPE(PointEvent event) {
        lastAlphabet = currentAlphabet;
        currentAlphabet = classifier.classify(event); // 将事件分类为相应的字母（Alphabet）
        dfa.step(currentAlphabet); // 根据字母推进DFA的状态
        recordEndPoint(event); // 根据当前的状态记录事件的起止点
        return currentAlphabet;
    }

    /**
     * 根据当前状态记录前后Pie的起始和结束事件。
     *
     * @param event 当前处理的事件
     */
    private void recordEndPoint(PointEvent event) {
        if (dfa.isStateChanged()) { // 仅当状态改变时才记录事件
            if (isFormerPieStartTransition()) {
                formerPieStart = event;
            }
            if (isFormerPieEndTransition()) {
                formerPieEnd = event;
            }
            if (isLatterPieStartTransition()) {
                latterPieStart = event;
            }
            if (isLatterPieEndTransition()) {
                latterPieEnd = event;
            }
        }
    }

    /**
     * 判断当前状态是否为formerPie的开始转换。
     *
     * @return 如果是formerPie的开始转换则返回true，否则返回false
     */
    private boolean isFormerPieStartTransition( ) {
        return (lastAlphabet == Alphabet.O || lastAlphabet == Alphabet.I || lastAlphabet == null) &&
                (currentAlphabet == Alphabet.Z || currentAlphabet == Alphabet.E);
    }

    /**
     * 判断当前状态是否为formerPie的结束转换。
     *
     * @return 如果是formerPie的结束转换则返回true，否则返回false
     */
    private boolean isFormerPieEndTransition( ) {
        return (lastAlphabet == Alphabet.Z || lastAlphabet == Alphabet.E) &&
                (currentAlphabet == Alphabet.O || currentAlphabet == Alphabet.I);
    }

    /**
     * 判断当前状态是否为latterPie的开始转换。
     *
     * @return 如果是latterPie的开始转换则返回true，否则返回false
     */
    private boolean isLatterPieStartTransition( ) {
        return (lastAlphabet == Alphabet.O || lastAlphabet == Alphabet.Z || lastAlphabet == null) &&
                (currentAlphabet == Alphabet.I || currentAlphabet == Alphabet.E);
    }

    /**
     * 判断当前状态是否为latterPie的结束转换。
     *
     * @return 如果是latterPie的结束转换则返回true，否则返回false
     */
    private boolean isLatterPieEndTransition( ) {
        return (lastAlphabet == Alphabet.I || lastAlphabet == Alphabet.E) &&
                (currentAlphabet == Alphabet.O || currentAlphabet == Alphabet.Z);
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
        return dfa.isCompleted();
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
}
