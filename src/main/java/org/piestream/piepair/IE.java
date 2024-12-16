package org.piestream.piepair;

import org.piestream.events.Expirable;
import org.piestream.events.PointEvent;
import org.piestream.piepair.eba.EBA;

import java.util.Objects;

public class IE implements Expirable   {

    private final EBA pred;
    private final PointEvent startEvent;    // 开始事件
    private PointEvent endEvent;      // 结束事件 (可以为 null)
    private final Long triggerTime;
    private final Long startTime;     // 开始事件的时间
    private Long endTime;     // 结束事件的开始时间 (可以为 null)



    // 构造函数
    public IE(EBA pred, PointEvent startEvent, PointEvent endEvent,long triggerTime ) {
        this.pred = pred;
        this.startEvent = Objects.requireNonNull(startEvent, "startEvent cannot be null");
        this.endEvent = endEvent;
        this.startTime = Objects.requireNonNull(startEvent.getTimestamp(), "startEvent cannot be null");
        this.endTime = endEvent!=null?endEvent.getTimestamp():null;
        this.triggerTime=triggerTime;
    }


    // Getter 和 Setter 方法
    public EBA getPred() {
        return pred;
    }


    public PointEvent getStartEvent() {
        return startEvent;
    }


    public PointEvent getEndEvent() {
        return endEvent;
    }

    public void setEndEvent(PointEvent endEvent) {
        this.endEvent = endEvent;
    }

    public Long getStartTime() {
        return startTime;
    }


    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

//    @Override
//    public  boolean isExpired(long deadLine){
//        return triggerTime<deadLine;
//    }

    @Override
    public  boolean isExpired(long deadLine){
        return startTime<deadLine;
    }

    @Override
    public long getSortKey() {
        return triggerTime;
    }

}
