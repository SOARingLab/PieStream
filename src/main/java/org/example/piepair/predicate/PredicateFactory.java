package org.example.piepair.predicate;

public class PredicateFactory {

    public static Predicate fromOperator(String operator) {
        switch (operator.toLowerCase()) {
            case "equals":
            case "=":
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
