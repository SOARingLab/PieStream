package org.example.piepair;

import java.util.Objects;
import org.example.events.PointEvent;

public class IEP {
    private TemporalRelations.AllRel relation; // 时间关系
    private PointEvent formerPieStart;         // formerPie的开始事件
    private PointEvent latterPieStart;         // latterPie的开始事件 (可以为 null)
    private PointEvent formerPieEnd;           // formerPie的结束事件
    private PointEvent latterPieEnd;           // latterPie的结束事件 (可以为 null)
    private Long formerStartTime;     // 前事件的开始时间
    private Long latterStartTime;     // 后事件的开始时间
    private boolean isCompleted;

    // 构造函数
    public IEP(TemporalRelations.AllRel relation,
               PointEvent formerPieStart,
               PointEvent latterPieStart,
               PointEvent formerPieEnd,
               PointEvent latterPieEnd,
               Long formerStartTime,
               Long latterStartTime) {

        // 对不允许为null的参数进行检查
        this.relation = Objects.requireNonNull(relation, "relation cannot be null");
        this.formerPieStart = Objects.requireNonNull(formerPieStart, "formerPieStart cannot be null");
        this.latterPieStart = Objects.requireNonNull(latterPieStart, "latterPieStart cannot be null");
        this.formerStartTime = Objects.requireNonNull(formerStartTime, "formerStartTime cannot be null");
        this.latterStartTime = Objects.requireNonNull(latterStartTime, "latterStartTime cannot be null");

        // 允许 formerPieEnd 和 latterPieEnd 为 null
        this.formerPieEnd = formerPieEnd;
        this.latterPieEnd = latterPieEnd;

        this.isCompleted=false;
    }
    // 构造函数，从 PreciseRel 构造
    public IEP(TemporalRelations.PreciseRel relation,
               PointEvent formerPieStart,
               PointEvent latterPieStart,
               PointEvent formerPieEnd,
               PointEvent latterPieEnd,
               Long formerStartTime,
               Long latterStartTime) {

        // 对不允许为null的参数进行检查
        this(TemporalRelations.AllRel.fromPreciseRel(relation),formerPieStart,latterPieStart,formerPieEnd,latterPieEnd,formerStartTime,latterStartTime);
    }
    // 构造函数，从 AllenRel 构造
    public IEP(TemporalRelations.AllenRel relation,
               PointEvent formerPieStart,
               PointEvent latterPieStart,
               PointEvent formerPieEnd,
               PointEvent latterPieEnd,
               Long formerStartTime,
               Long latterStartTime) {

        // 对不允许为null的参数进行检查
        this(TemporalRelations.AllRel.fromAllenRel(relation),formerPieStart,latterPieStart,formerPieEnd,latterPieEnd,formerStartTime,latterStartTime);
    }



    // Getters and setters
    public TemporalRelations.AllRel getRelation() {
        return relation;
    }

    public void setRelation(TemporalRelations.AllRel relation) {
        this.relation = relation;
    }

    public PointEvent getFormerPieStart() {
        return formerPieStart;
    }

    public void setFormerPieStart(PointEvent formerPieStart) {
        this.formerPieStart = formerPieStart;
    }

    public PointEvent getLatterPieStart() {
        return latterPieStart;
    }

    public void setLatterPieStart(PointEvent latterPieStart) {
        this.latterPieStart = latterPieStart;
    }

    public PointEvent getFormerPieEnd() {
        return formerPieEnd;
    }

    public void setFormerPieEnd(PointEvent formerPieEnd) {
        this.formerPieEnd = formerPieEnd;
    }

    public PointEvent getLatterPieEnd() {
        return latterPieEnd;
    }

    public void setLatterPieEnd(PointEvent latterPieEnd) {
        this.latterPieEnd = latterPieEnd;
    }

    public Long getFormerStartTime() {
        return formerStartTime;
    }

    public void setFormerStartTime(Long formerStartTime) {
        this.formerStartTime = formerStartTime;
    }

    public Long getLatterStartTime() {
        return latterStartTime;
    }

    public void setLatterStartTime(Long latterStartTime) {
        this.latterStartTime = latterStartTime;
    }

    @Override
    public String toString() {
        String formerPieEndTime=formerPieEnd==null?"not finish": String.valueOf(formerPieEnd.getTimestamp());
        String latterPieEndTime=latterPieEnd==null?"not finish": String.valueOf(latterPieEnd.getTimestamp());
        return "IEP{" +
                relation +
                ", " + formerPieStart.getTimestamp() +
                ", " + latterPieStart.getTimestamp() +
                ", " + formerPieEndTime +
                ", " + latterPieEndTime +
                '}';
    }
    @Override
    public int hashCode() {
        return Objects.hash(relation, formerPieStart, latterPieStart, formerPieEnd, latterPieEnd, formerStartTime, latterStartTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IEP iep = (IEP) o;

        // 如果 hashCode 不相等，直接返回 false
        if (this.hashCode() != iep.hashCode()) {
            return false;
        }

        // 进一步比较所有字段
        return relation == iep.relation &&
                Objects.equals(formerPieStart, iep.formerPieStart) &&
                Objects.equals(latterPieStart, iep.latterPieStart) &&
                Objects.equals(formerPieEnd, iep.formerPieEnd) &&
                Objects.equals(latterPieEnd, iep.latterPieEnd) &&
                Objects.equals(formerStartTime, iep.formerStartTime) &&
                Objects.equals(latterStartTime, iep.latterStartTime);
    }
    public void complete(){
        this.isCompleted =true;
    }

}