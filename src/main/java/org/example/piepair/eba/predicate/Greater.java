package org.example.piepair.eba.predicate;

import org.example.events.Attribute;
import org.example.events.PointEvent;

public class Greater implements Predicate {
    @Override
    public boolean test(PointEvent event, Attribute attribute, Object parameter) {
        // 从 PointEvent 的 payload 中获取属性值
        Object attributeValue = event.getPayload().get(attribute);

        // 检查属性值是否为 null 或者参数是否为 null
        if (attributeValue == null || parameter == null) {
            return false;
        }

        try {
            // 将属性值和参数转换为 double 类型进行比较
            double attributeDoubleValue = Double.parseDouble(attributeValue.toString());
            double parameterDoubleValue = Double.parseDouble(parameter.toString());
            return attributeDoubleValue > parameterDoubleValue;
        } catch (NumberFormatException e) {
            // 如果转换失败，返回 false
            return false;
        }
    }
}
