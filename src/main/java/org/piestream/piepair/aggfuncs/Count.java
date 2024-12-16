package org.piestream.piepair.aggfuncs;

public class Count implements AggregationFunction<Long> {
    private long count = 0; // 计数器

    @Override
    public void update(Long value) {
        count++; // 每次新事件到来时，计数器增加1
    }

    @Override
    public Long getResult() {
        return count; // 返回当前计数
    }

    @Override
    public void reset() {
        count = 0; // 重置计数器
    }
}
