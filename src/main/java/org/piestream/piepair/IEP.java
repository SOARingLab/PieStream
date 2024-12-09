package org.piestream.piepair;

import java.util.Objects;

import org.piestream.events.Expirable;
import org.piestream.events.PointEvent;
import org.piestream.piepair.eba.EBA;

public class IEP implements Expirable {
    private EBA formerPie;
    private EBA latterPie;
    private TemporalRelations.AllRel relation; // 时间关系
    private PointEvent formerPieStart;         // formerPie的开始事件
    private PointEvent latterPieStart;         // latterPie的开始事件 (可以为 null)
    private PointEvent formerPieEnd;           // formerPie的结束事件
    private PointEvent latterPieEnd;           // latterPie的结束事件 (可以为 null)
    private Long formerStartTime;     // 前事件的开始时间
    private Long latterStartTime;     // 后事件的开始时间
    private PointEvent triggerEvent;
    private Long triggerTime;
    private Long systemTriggerTime;
    private CompletedTime compTime;
//    private boolean isCompleted;

    public enum CompletedTime {
        FormerEnd,
        LatterEnd,
        NoNeed
    }
    // 构造函数
    public IEP(TemporalRelations.AllRel relation,
               EBA formerPie,
               EBA latterPie,
               PointEvent formerPieStart,
               PointEvent latterPieStart,
               PointEvent formerPieEnd,
               PointEvent latterPieEnd,
               Long formerStartTime,
               Long latterStartTime,
               PointEvent triggerEvent,
               Long triggerTime) {

        // 对不允许为null的参数进行检查
        this.relation = Objects.requireNonNull(relation, "relation cannot be null");
        this.formerPieStart = Objects.requireNonNull(formerPieStart, "formerPieStart cannot be null");
        this.latterPieStart = Objects.requireNonNull(latterPieStart, "latterPieStart cannot be null");
        this.formerStartTime = Objects.requireNonNull(formerStartTime, "formerStartTime cannot be null");
        this.latterStartTime = Objects.requireNonNull(latterStartTime, "latterStartTime cannot be null");
        this.triggerEvent = Objects.requireNonNull(triggerEvent, "triggerEvent cannot be null");
        this.triggerTime = Objects.requireNonNull(triggerTime, "triggerTime cannot be null");


        // 允许 formerPieEnd 和 latterPieEnd 为 null
        this.formerPieEnd = formerPieEnd;
        this.latterPieEnd = latterPieEnd;
        this.formerPie = formerPie;
        this.latterPie = latterPie;
        this.compTime=determinCompTimeByRel();
        this.systemTriggerTime= System.nanoTime();

//        this.isCompleted=false;
    }

    // 构造函数，从 PreciseRel 构造
    public IEP(TemporalRelations.PreciseRel relation,
               EBA formerPie,
               EBA latterPie,
               PointEvent formerPieStart,
               PointEvent latterPieStart,
               PointEvent formerPieEnd,
               PointEvent latterPieEnd,
               Long formerStartTime,
               Long latterStartTime,
               PointEvent triggerEvent,
               Long triggerTime) {

        // 对不允许为null的参数进行检查
        this(TemporalRelations.AllRel.fromPreciseRel(relation),formerPie,latterPie,formerPieStart,latterPieStart,formerPieEnd,latterPieEnd,formerStartTime,latterStartTime,triggerEvent,triggerTime);
    }
    // 构造函数，从 AllenRel 构造
    public IEP(TemporalRelations.AllenRel relation,
               EBA formerPie,
               EBA latterPie,
               PointEvent formerPieStart,
               PointEvent latterPieStart,
               PointEvent formerPieEnd,
               PointEvent latterPieEnd,
               Long formerStartTime,
               Long latterStartTime,
               PointEvent triggerEvent,
               Long triggerTime) {

        // 对不允许为null的参数进行检查
        this(TemporalRelations.AllRel.fromAllenRel(relation),formerPie,latterPie,formerPieStart,latterPieStart,formerPieEnd,latterPieEnd,formerStartTime,latterStartTime,triggerEvent,triggerTime);
    }

    public EBA getFormerPie(){
            return formerPie;
    }

    public EBA getLatterPie(){
        return latterPie;
    }

    // Getters and setters
    public TemporalRelations.AllRel getRelation() {
        return relation;
    }


    public Long getSystemTriggerTime() {
        return systemTriggerTime;
    }

    public PointEvent getFormerPieStart() {
        return formerPieStart;
    }


    public CompletedTime getCompTime() {
        return compTime;
    }

    public PointEvent getLatterPieStart() {
        return latterPieStart;
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



    public void setFormerStartTime(Long formerStartTime) {
        this.formerStartTime = formerStartTime;
    }

    public Long getFormerStartTime() {
        return formerStartTime ;
    }


    public Long getLatterStartTime() {
        return latterStartTime ;
    }

    public Long getFormerEndTime() {
        if (formerPieEnd==null){
            return 0L;
        }
        else{
            return  formerPieEnd.getTimestamp();
        }
    }

    public Long getLatterEndTime() {
        if (latterPieEnd==null){
            return 0L;
        }
        else{
            return latterPieEnd.getTimestamp();
        }
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

    private CompletedTime determinCompTimeByRel(){
        if (relation.isPreciseRel()){
            TemporalRelations.PreciseRel preRel=relation.getPreciseRel();
            if(preRel.triggerWithoutLatterPieEnd()){
                return CompletedTime.LatterEnd;
            } else if (preRel.triggerWithoutFormerPieEnd()) {
                return CompletedTime.FormerEnd;
            }
            else if(preRel.triggerWithCompleted()) {
                return CompletedTime.NoNeed;
            }
            else{
                throw new IllegalStateException("Unexpected precise relation state.");

            }
        }else{
            if(relation.getAllenRel()== TemporalRelations.AllenRel.AFTER){
                return  CompletedTime.FormerEnd;
            } else if (relation.getAllenRel()==TemporalRelations.AllenRel.BEFORE) {
                return CompletedTime.LatterEnd;
            }else{
                throw new IllegalStateException("Unexpected precise relation state.");

            }
        }
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

    public Long getTriggerTime(){
        return triggerTime;
    }

    public Long getStartTime(EBA pred) {
        if (pred == formerPie) {
            return formerStartTime;
        } else if (pred == latterPie) {
            return latterStartTime;
        } else {
            throw new IllegalArgumentException("The provided EBA predicate does not match formerPie or latterPie.");
        }
    }

    @Override
    public  boolean isExpired(long deadLine){
        return triggerTime<deadLine;
    }

    @Override
    public long getSortKey() {
        return triggerTime;
    }
}