package org.piestream.piepair.aggfuncs;

public interface AggregationFunction<T> {
    // 更新聚合器状态，通常在每次新事件到来时调用
    void update(T value);

    // 获取当前聚合结果
    T getResult();

    // 重置聚合器
    void reset();
}
