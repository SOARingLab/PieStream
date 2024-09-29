package org.example.piepair;

import org.example.events.PointEvent;
import org.example.piepair.eba.EBA;

import java.util.Objects;

public class IE {

    private EBA pred;
    private PointEvent startEvent;    // 开始事件
    private PointEvent endEvent;      // 结束事件 (可以为 null)

    private Long startTime;     // 开始事件的时间
    private Long endTime;     // 结束事件的开始时间 (可以为 null)

    // 构造函数
    public IE(EBA pred, PointEvent startEvent, PointEvent endEvent, Long startTime, Long endTime) {
        this.pred = pred;
        this.startEvent = Objects.requireNonNull(startEvent, "startEvent cannot be null");
        this.endEvent = endEvent;
        this.startTime = Objects.requireNonNull(startTime, "startEvent cannot be null");
        this.endTime = endTime;
    }

    // 构造函数
    public IE(EBA pred, PointEvent startEvent, PointEvent endEvent ) {
        this.pred = pred;
        this.startEvent = Objects.requireNonNull(startEvent, "startEvent cannot be null");
        this.endEvent = endEvent;
        this.startTime = Objects.requireNonNull(startEvent.getTimestamp(), "startEvent cannot be null");
        this.endTime = endEvent!=null?endEvent.getTimestamp():null;
    }

    public IE(EBA pred, PointEvent startEvent,   Long startTime  ) {
        this(pred,startEvent,null,startTime,null);
    }

    public IE(EBA pred, PointEvent startEvent   ) {
        this(pred,startEvent,null,startEvent.getTimestamp(),null);
    }

    // Getter 和 Setter 方法
    public EBA getPred() {
        return pred;
    }

    public void setPred(EBA pred) {
        this.pred = pred;
    }

    public PointEvent getStartEvent() {
        return startEvent;
    }

    public void setStartEvent(PointEvent startEvent) {
        this.startEvent = startEvent;
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

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }
}
