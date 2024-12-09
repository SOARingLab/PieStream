package org.piestream.piepair.eba;

import org.piestream.events.Attribute;
import org.piestream.parser.Schema;
import org.piestream.piepair.predicate.Predicate;
import org.piestream.piepair.predicate.PredicateUtils;

import java.util.List;
import java.util.Stack;

public class EBAParser {

    public static EBA parse(String expression, Schema schema) throws EBA.ParseException {
        expression = expression.trim();
        Stack<EBA> operands = new Stack<>();
        Stack<Character> operators = new Stack<>();

        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);

            if (Character.isWhitespace(c)) {
                continue;
            }

            if (c == '(') {
                operators.push(c);
            } else if (c == ')') {
                processClosingBracket(operands, operators);
            } else if (isOperator(c)) {
                processOperator(operands, operators, c);
            } else {
                String predicate = extractPredicate(expression, i);
                i += predicate.length() - 1; // Adjust loop index
                operands.push( parsePredicateExpression(predicate.trim(),schema));
            }
        }

        while (!operators.isEmpty()) {
            applyOperator(operands, operators.pop());
        }

        return operands.pop();
    }

    private static void processClosingBracket(Stack<EBA> operands, Stack<Character> operators) throws EBA.ParseException {
        while (!operators.isEmpty() && operators.peek() != '(') {
            applyOperator(operands, operators.pop());
        }
        operators.pop();
    }

    private static void processOperator(Stack<EBA> operands, Stack<Character> operators, char currentOperator) throws EBA.ParseException {
        while (!operators.isEmpty() && precedence(currentOperator) <= precedence(operators.peek())) {
            applyOperator(operands, operators.pop());
        }
        operators.push(currentOperator);
    }

    private static String extractPredicate(String expression, int startIndex) {
        StringBuilder sb = new StringBuilder();
        while (startIndex < expression.length() && !isOperator(expression.charAt(startIndex))) {
            sb.append(expression.charAt(startIndex++));
        }
        return sb.toString();
    }

    private static void applyOperator(Stack<EBA> operands, char operator) throws EBA.ParseException {
        EBA right = operands.pop();
        EBA left = (operator != '!') ? operands.pop() : null;
        operands.push(createOperatorEBA(operator, left, right));
    }

    private static boolean isOperator(char c) {
        return c == '&' || c == '|' || c == '!' || c == '(' || c == ')';
    }

    private static int precedence(char operator) {
        switch (operator) {
            case '!':
                return 3;
            case '&':
                return 2;
            case '|':
                return 1;
            default:
                return -1;
        }
    }

    private static EBA createOperatorEBA(char operator, EBA left, EBA right) throws EBA.ParseException {
        switch (operator) {
            case '&':
                return new  AndEBA(left, right);
            case '|':
                return new  OrEBA(left, right);
            case '!':
                return new  NotEBA(right);
            default:
                throw new EBA.ParseException("Unknown operator: " + operator);
        }
    }

    // 改进后的 parsePredicateExpression 方法
    private static EBA parsePredicateExpression(String expression, Schema schema) throws EBA.ParseException {
        String[] parts = splitPredicateExpression(expression);
        String attributeStr = parts[0];
        String operator = parts[1];
        String value = parts[2];

        Attribute attribute = new Attribute(attributeStr, finadValueType(attributeStr,schema));
        Predicate predicate = PredicateUtils.fromOperator(operator);
        Object parameter = parseValue(value);

        return new  PredicateEBA(predicate, attribute, parameter);
    }

    private static String finadValueType(String attributeStr,Schema schema) throws EBA.ParseException {
        List<Attribute> Attris=schema.getAttributes();
        for (Attribute attri:Attris){
            if (attri.getName().toLowerCase().equals(attributeStr.toLowerCase())){
                return attri.getType().toLowerCase();
            }
        }
        throw new EBA.ParseException("can't find right type: " + attributeStr );
    }
    // 确定值类型
    private static String determineValueType(String value) {
        if (value.matches("-?\\d+")) {
            return "int";
        } else if (value.matches("-?\\d*\\.\\d+")) {
            return "float";
        } else {
            return "String";
        }
    }

    // Helper method to convert a string to an appropriate type (e.g., number, string)
    private static Object parseValue(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e1) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e2) {
                return value; // Fallback to string
            }
        }
    }

    // 分割谓词表达式为 attribute、operator 和 value
    private static String[] splitPredicateExpression(String expression) throws EBA.ParseException {
        String[] parts = expression.trim().split("\\s+");
        if (parts.length != 3) {
            throw new EBA.ParseException("Invalid predicate expression: " + expression);
        }
        return parts;
    }


}
