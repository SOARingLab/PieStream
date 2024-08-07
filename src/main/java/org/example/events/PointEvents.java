package org.example.events;

import java.util.Iterator;

public interface PointEvents extends Iterable<PointEvent> {
    // 定义返回 PointEvent 迭代器的方法
    Iterator<PointEvent> iterator();
}