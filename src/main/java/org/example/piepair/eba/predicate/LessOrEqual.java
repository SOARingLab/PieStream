package org.example.piepair.eba.predicate;

import org.example.events.Attribute;
import org.example.events.PointEvent;

public class LessOrEqual implements Predicate {
    @Override
    public boolean test(PointEvent event, Attribute attribute, Object parameter) {
        // 从 PointEvent 的 payload 中获取属性值
        Object attributeValue = event.getPayload().get(attribute);

        // 检查属性值和参数是否为 null
        if (attributeValue == null || parameter == null) {
            return false;
        }

        try {
            // 检查 attributeValue 和 parameter 是否是数字类型
            if (!(attributeValue instanceof Number) || !(parameter instanceof Number)) {
                return false;
            }

            // 将属性值和参数转换为 double 类型进行比较
            double attributeDoubleValue = ((Number) attributeValue).doubleValue();
            double parameterDoubleValue = ((Number) parameter).doubleValue();
            return attributeDoubleValue <= parameterDoubleValue;
        } catch (ClassCastException e) {
            // 如果类型转换失败，返回 false
            return false;
        }
    }
}
