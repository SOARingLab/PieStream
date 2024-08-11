package org.example.piepair.eba.predicate;

import org.example.events.Attribute;
import org.example.events.PointEvent;

public class Equals implements Predicate {

    @Override
    public boolean test(PointEvent event, Attribute attribute, Object parameter) {
        // 从 PointEvent 的 payload 中获取属性值
        Object attributeValue = event.getPayload().get(attribute);

        // 检查属性值是否为 null，如果是，直接返回 false
        if (attributeValue == null) {
            return false;
        }

        // 使用 equals 方法比较属性值和参数
        return attributeValue.toString().equals(parameter.toString());
    }
}
