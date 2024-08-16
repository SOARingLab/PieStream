package org.example.events;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class PointEventIterator implements Iterator<PointEvent> {
    private final List<PointEvent> events;
    private int currentIndex;

    public PointEventIterator() {
        this.events = new LinkedList<>();
        this.currentIndex = 0;
    }

    // 添加事件到列表
    public void addEvent(PointEvent event) {
        events.add(event);
    }

    // 检查是否还有下一个事件
    @Override
    public boolean hasNext() {
        return currentIndex < events.size();
    }

    // 获取下一个事件
    @Override
    public PointEvent next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more events");
        }
        return events.get(currentIndex++);
    }

    // 移除当前事件
    @Override
    public void remove() {
        if (currentIndex <= 0 || currentIndex > events.size()) {
            throw new IllegalStateException("Invalid state for removal");
        }
        events.remove(--currentIndex);
    }

    // 重置迭代器
    public void reset() {
        currentIndex = 0;
    }

    // 获取事件列表的大小
    public int size() {
        return events.size();
    }
}
