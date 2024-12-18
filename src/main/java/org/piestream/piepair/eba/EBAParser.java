package org.piestream.piepair.eba;

import org.piestream.events.Attribute;
import org.piestream.parser.Schema;
import org.piestream.piepair.predicate.Predicate;
import org.piestream.piepair.predicate.PredicateUtils;

import java.util.List;
import java.util.Stack;

/**
 * This class parses an event-based analysis (EBA) expression and constructs the corresponding EBA tree.
 * The EBA expression consists of predicates and logical operators like AND, OR, and NOT.
 */
public class EBAParser {

    /**
     * Parses an EBA expression and returns an EBA object representing the parsed logic.
     * The expression consists of predicates and logical operators, including parentheses for grouping.
     *
     * @param expression the EBA expression to parse.
     * @param schema the schema that defines the attributes and their types.
     * @return the EBA object representing the parsed expression.
     * @throws EBA.ParseException if there is an error during parsing.
     */
    public static EBA parse(String expression, Schema schema) throws EBA.ParseException {
        expression = expression.trim();
        Stack<EBA> operands = new Stack<>();
        Stack<Character> operators = new Stack<>();

        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);

            // Skip whitespace characters
            if (Character.isWhitespace(c)) {
                continue;
            }

            // Handle opening parentheses
            if (c == '(') {
                operators.push(c);
            }
            // Handle closing parentheses
            else if (c == ')') {
                processClosingBracket(operands, operators);
            }
            // Handle operators (&, |, !)
            else if (isOperator(c)) {
                processOperator(operands, operators, c);
            }
            // Handle predicates (e.g., attribute comparisons)
            else {
                String predicate = extractPredicate(expression, i);
                i += predicate.length() - 1; // Adjust the loop index after extracting the predicate
                operands.push(parsePredicateExpression(predicate.trim(), schema));
            }
        }

        // Apply remaining operators
        while (!operators.isEmpty()) {
            applyOperator(operands, operators.pop());
        }

        return operands.pop();
    }

    /**
     * Processes a closing parenthesis, applying operators until an opening parenthesis is encountered.
     *
     * @param operands the stack of operands (EBAs).
     * @param operators the stack of operators.
     * @throws EBA.ParseException if there is a parsing error.
     */
    private static void processClosingBracket(Stack<EBA> operands, Stack<Character> operators) throws EBA.ParseException {
        while (!operators.isEmpty() && operators.peek() != '(') {
            applyOperator(operands, operators.pop());
        }
        operators.pop(); // Remove the opening parenthesis
    }

    /**
     * Processes an operator by applying operators with higher or equal precedence.
     *
     * @param operands the stack of operands (EBAs).
     * @param operators the stack of operators.
     * @param currentOperator the current operator to process.
     * @throws EBA.ParseException if there is a parsing error.
     */
    private static void processOperator(Stack<EBA> operands, Stack<Character> operators, char currentOperator) throws EBA.ParseException {
        while (!operators.isEmpty() && precedence(currentOperator) <= precedence(operators.peek())) {
            applyOperator(operands, operators.pop());
        }
        operators.push(currentOperator);
    }

    /**
     * Extracts a predicate (i.e., attribute comparison) from the expression starting at the specified index.
     *
     * @param expression the full expression.
     * @param startIndex the starting index of the predicate in the expression.
     * @return the extracted predicate as a string.
     */
    private static String extractPredicate(String expression, int startIndex) {
        StringBuilder sb = new StringBuilder();
        while (startIndex < expression.length() && !isOperator(expression.charAt(startIndex))) {
            sb.append(expression.charAt(startIndex++));
        }
        return sb.toString();
    }

    /**
     * Applies an operator to the operands stack.
     * This creates a new EBA object based on the operator and operands.
     *
     * @param operands the stack of operands (EBAs).
     * @param operator the operator to apply.
     * @throws EBA.ParseException if there is an invalid operator.
     */
    private static void applyOperator(Stack<EBA> operands, char operator) throws EBA.ParseException {
        EBA right = operands.pop();
        EBA left = (operator != '!') ? operands.pop() : null;
        operands.push(createOperatorEBA(operator, left, right));
    }

    /**
     * Checks if the given character is a valid operator.
     *
     * @param c the character to check.
     * @return true if the character is an operator, false otherwise.
     */
    private static boolean isOperator(char c) {
        return c == '&' || c == '|' || c == '!' || c == '(' || c == ')';
    }

    /**
     * Returns the precedence of the given operator.
     *
     * @param operator the operator to check.
     * @return the precedence value of the operator.
     */
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

    /**
     * Creates an EBA object based on the operator and its left and right operands.
     *
     * @param operator the operator to apply.
     * @param left the left operand (can be null for the NOT operator).
     * @param right the right operand.
     * @return the created EBA object.
     * @throws EBA.ParseException if the operator is unknown.
     */
    private static EBA createOperatorEBA(char operator, EBA left, EBA right) throws EBA.ParseException {
        switch (operator) {
            case '&':
                return new AndEBA(left, right);
            case '|':
                return new OrEBA(left, right);
            case '!':
                return new NotEBA(right);
            default:
                throw new EBA.ParseException("Unknown operator: " + operator);
        }
    }

    /**
     * Parses a predicate expression (e.g., attribute comparison) into an EBA object.
     *
     * @param expression the predicate expression to parse.
     * @param schema the schema containing attribute information.
     * @return the EBA object representing the parsed predicate.
     * @throws EBA.ParseException if there is a parsing error.
     */
    private static EBA parsePredicateExpression(String expression, Schema schema) throws EBA.ParseException {
        String[] parts = splitPredicateExpression(expression);
        String attributeStr = parts[0];
        String operator = parts[1];
        String value = parts[2];

        Attribute attribute = new Attribute(attributeStr, findValueType(attributeStr, schema));
        Predicate predicate = PredicateUtils.fromOperator(operator);
        Object parameter = parseValue(value);

        return new PredicateEBA(predicate, attribute, parameter);
    }

    /**
     * Finds the data type of the specified attribute in the schema.
     *
     * @param attributeStr the name of the attribute.
     * @param schema the schema containing the attribute information.
     * @return the data type of the attribute.
     * @throws EBA.ParseException if the attribute is not found in the schema.
     */
    private static String findValueType(String attributeStr, Schema schema) throws EBA.ParseException {
        List<Attribute> attributes = schema.getAttributes();
        for (Attribute attr : attributes) {
            if (attr.getName().equalsIgnoreCase(attributeStr)) {
                return attr.getType().toLowerCase();
            }
        }
        throw new EBA.ParseException("Cannot find the correct type for attribute: " + attributeStr);
    }

    /**
     * Determines the type of a value (int, float, or String).
     *
     * @param value the value to check.
     * @return the type of the value as a string.
     */
    private static String determineValueType(String value) {
        if (value.matches("-?\\d+")) {
            return "int";
        } else if (value.matches("-?\\d*\\.\\d+")) {
            return "float";
        } else {
            return "String";
        }
    }

    /**
     * Converts a string value into the appropriate type (int, float, or String).
     *
     * @param value the value to parse.
     * @return the parsed value as an Object (either Integer, Double, or String).
     */
    private static Object parseValue(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e1) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e2) {
                return value; // Fallback to string if not an integer or float
            }
        }
    }

    /**
     * Splits a predicate expression into three parts: attribute, operator, and value.
     *
     * @param expression the predicate expression to split.
     * @return an array containing the attribute, operator, and value.
     * @throws EBA.ParseException if the predicate expression is invalid.
     */
    private static String[] splitPredicateExpression(String expression) throws EBA.ParseException {
        String[] parts = expression.trim().split("\\s+");
        if (parts.length != 3) {
            throw new EBA.ParseException("Invalid predicate expression: " + expression);
        }
        return parts;
    }
}
