package org.example.piepair.predicate;

public class PredicateUtils {
    public static boolean compareValues(Object attributeValue, Object parameter, String type, String operator) {
        if (attributeValue == null || parameter == null) {
            return false;
        }
        try {
            switch (type.toLowerCase()) {
                case "int":
                    int intVal1 = Integer.parseInt(attributeValue.toString());
                    int intVal2 = Integer.parseInt(parameter.toString());
                    return compare(intVal1, intVal2, operator);
                case "long":
                    long longVal1 = Long.parseLong(attributeValue.toString());
                    long longVal2 = Long.parseLong(parameter.toString());
                    return compare(longVal1, longVal2, operator);
                case "float":
                    float floatVal1 = Float.parseFloat(attributeValue.toString());
                    float floatVal2 = Float.parseFloat(parameter.toString());
                    return compare(floatVal1, floatVal2, operator);
                case "double":
                    double doubleVal1 = Double.parseDouble(attributeValue.toString());
                    double doubleVal2 = Double.parseDouble(parameter.toString());
                    return compare(doubleVal1, doubleVal2, operator);
                case "byte":
                    double byteVal1 = Double.parseDouble(attributeValue.toString());
                    double byteVal2 = Double.parseDouble(parameter.toString());
                    return compare(byteVal1, byteVal2, operator);
                default:
                    throw new IllegalArgumentException("Unsupported attribute type: " + type);
            }
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // 重载 compare 方法
    public static boolean compare(int a, int b, String operator) {
        switch (operator) {
            case ">":  return a > b;
            case ">=": return a >= b;
            case "<":  return a < b;
            case "<=": return a <= b;
            case "==": return a == b;
            case "!=": return a != b;
            default:   throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }

    public static boolean compare(long a, long b, String operator) {
        // 实现同上
        switch (operator) {
            case ">":  return a > b;
            case ">=": return a >= b;
            case "<":  return a < b;
            case "<=": return a <= b;
            case "==": return a == b;
            case "!=": return a != b;
            default:   throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }

    public static boolean compare(float a, float b, String operator) {
        // 实现同上
        switch (operator) {
            case ">":  return a > b;
            case ">=": return a >= b;
            case "<":  return a < b;
            case "<=": return a <= b;
            case "==": return a == b;
            case "!=": return a != b;
            default:   throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }

    public static boolean compare(double a, double b, String operator) {
        // 实现同上
        switch (operator) {
            case ">":  return a > b;
            case ">=": return a >= b;
            case "<":  return a < b;
            case "<=": return a <= b;
            case "==": return a == b;
            case "!=": return a != b;
            default:   throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }
    public static Predicate fromOperator(String operator) {
        switch (operator.toLowerCase()) {
            case "equals":
            case "=":
                return new Equals();
            case "==":
                return new Equals();
            case "greater":
            case ">":
                return new Greater();
            case "greaterorequal":
            case ">=":
                return new GreaterOrEqual();
            case "less":
            case "<":
                return new Less();
            case "lessorequal":
            case "<=":
                return new LessOrEqual();
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }
}
