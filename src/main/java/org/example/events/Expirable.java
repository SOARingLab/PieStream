package org.example.events;


public interface Expirable {
    // 检查对象是否已过期
    boolean isExpired(long deadLine);
    long getSortKey();
}