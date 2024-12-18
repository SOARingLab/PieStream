package org.piestream.piepair.predicate;

/**
 * Utility class providing methods to compare values based on the specified type and operator.
 * It supports comparison operations on primitive types (int, long, float, double, byte)
 * and can be used to evaluate conditions in predicates.
 */
public class PredicateUtils {

    /**
     * Compares two values based on the specified type and operator.
     * The comparison is performed using the provided operator (e.g., ">", "<=", "==").
     *
     * @param attributeValue The value of the attribute to compare.
     * @param parameter The value to compare against.
     * @param type The type of the attribute (e.g., "int", "long", "float", etc.).
     * @param operator The operator to use for the comparison (e.g., ">", "<", "==").
     * @return True if the comparison holds according to the operator, false otherwise.
     * @throws IllegalArgumentException If the type is unsupported or the operator is invalid.
     */
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

    /**
     * Compares two integer values based on the specified operator.
     *
     * @param a The first integer value.
     * @param b The second integer value.
     * @param operator The comparison operator (e.g., ">", "<", "==").
     * @return True if the comparison holds, false otherwise.
     * @throws IllegalArgumentException If the operator is invalid.
     */
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

    /**
     * Compares two long values based on the specified operator.
     *
     * @param a The first long value.
     * @param b The second long value.
     * @param operator The comparison operator (e.g., ">", "<", "==").
     * @return True if the comparison holds, false otherwise.
     * @throws IllegalArgumentException If the operator is invalid.
     */
    public static boolean compare(long a, long b, String operator) {
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

    /**
     * Compares two float values based on the specified operator.
     *
     * @param a The first float value.
     * @param b The second float value.
     * @param operator The comparison operator (e.g., ">", "<", "==").
     * @return True if the comparison holds, false otherwise.
     * @throws IllegalArgumentException If the operator is invalid.
     */
    public static boolean compare(float a, float b, String operator) {
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

    /**
     * Compares two double values based on the specified operator.
     *
     * @param a The first double value.
     * @param b The second double value.
     * @param operator The comparison operator (e.g., ">", "<", "==").
     * @return True if the comparison holds, false otherwise.
     * @throws IllegalArgumentException If the operator is invalid.
     */
    public static boolean compare(double a, double b, String operator) {
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

    /**
     * Converts a string representation of an operator into a corresponding Predicate.
     *
     * @param operator The operator as a string (e.g., "equals", ">", "<=", etc.).
     * @return The corresponding Predicate object.
     * @throws IllegalArgumentException If the operator is not supported.
     */
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
