package org.example.piepair.predicate;

import org.example.events.Attribute;
import org.example.events.PointEvent;

public class Equals implements Predicate {

    @Override
    public boolean test(PointEvent event, Attribute attribute, Object parameter) {
        // 从 PointEvent 的 payload 中获取属性值
        Object attributeValue = event.getPayload().get(attribute);

        // 检查属性值是否为 null 或者参数是否为 null
        if (attributeValue == null || parameter == null) {
            return false;
        }

        try {
            // 根据属性的类型进行相应的比较
            switch (attribute.getType().toLowerCase()) {
                case "int":
                    return Integer.parseInt(attributeValue.toString()) == Integer.parseInt(parameter.toString());
                case "long":
                    return Long.parseLong(attributeValue.toString()) == Long.parseLong(parameter.toString());
                case "float":
                    return Float.parseFloat(attributeValue.toString()) == Float.parseFloat(parameter.toString());
                case "double":
                    return Double.parseDouble(attributeValue.toString()) == Double.parseDouble(parameter.toString());
                case "string":
                    return attributeValue.toString().equals(parameter.toString());
                default:
                    // 如果遇到不支持的类型，抛出异常或返回 false
                    throw new IllegalArgumentException("Unsupported attribute type: " + attribute.getType());
            }
        } catch (NumberFormatException e) {
            // 如果转换失败，返回 false
            return false;
        }
    }
}
