package org.piestream.parser;

import org.piestream.evaluation.Correct;
import org.piestream.piepair.TemporalRelations;
import org.piestream.piepair.eba.EBA;
import org.piestream.piepair.eba.EBAParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
/**
 * QueryParser class is responsible for parsing a query string in a specific format
 * used for temporal event processing. It splits the input query into different clauses
 * such as SELECT, FROM, DEFINE, PATTERN, and WITHIN. It validates the structure of the query,
 * tokenizes it, and then processes each clause for further analysis and execution.
 */
public class QueryParser {
    // Logger for logging error messages and debugging information
    private static final Logger logger = LoggerFactory.getLogger(QueryParser.class);

    // List of tokens parsed from the query string
    private final List<String> tokens;

    // Index to track the current token during parsing
    private int currentIndex = 0;

    // Schema representing the data structure
    private Schema schema;

    // List to store the 'RETURN' clause parsed from the query
    private List<String> returnClause = new ArrayList<>();

    // The 'FROM' clause defining the input stream of the query
    private String fromClause;

    // List to store the 'DEFINE' clause, which includes PIE definitions
    private List<Map<String, EBA>> defineClause = new ArrayList<>();

    // Map to store the mapping of PIEs (Event-Based Algebra) to their string representations (aliases)
    private Map<EBA, String> EBA2String = new HashMap<>();

    // List to store the parsed pattern clause, which defines the event pair pattern
    private List<MPIEPairSource> patternClause = new ArrayList<>(); // List of MPIEPairSource objects for pattern matching

    // Window size defined in the 'WITHIN' clause, in nanoseconds
    private long withinClause = 0;

    // The number of event pairs joined in the pattern matching
    private int JoinedNum = 0;

    // Set of predicates in the pattern clause
    private Set<EBA> predSetInPattern = new HashSet<EBA>();

    /**
     * Returns the parsed pattern clause, which defines the event pair pattern.
     *
     * @return A list of MPIEPairSource objects representing the pattern clause.
     */
    public List<MPIEPairSource> getPatternClause() {
        return this.patternClause;
    }

    /**
     * Returns the mapping of PIEs (Event-Based Algebra) to their string representations (aliases).
     *
     * @return A map where the keys are EBA objects and the values are their string aliases.
     */
    public Map<EBA, String> getEBA2String() {
        return this.EBA2String;
    }

    /**
     * Returns the number of event pairs joined in the pattern clause.
     *
     * @return The count of joined event pairs.
     */
    public int getJoinedNum() {
        return JoinedNum;
    }

    /**
     * Returns the number of predicates defined in the pattern clause.
     *
     * @return The number of predicates in the pattern.
     */
    public int getPredNumInPattern() {
        return predSetInPattern.size();
    }

    /**
     * Returns the window size defined in the 'WITHIN' clause, in nanoseconds.
     *
     * @return The window size in nanoseconds.
     */
    public long getwindowClause() {
        return this.withinClause;
    }

    /**
     * Constructs a QueryParser object with the given query string and schema.
     * This constructor tokenizes the query and prepares it for parsing.
     *
     * @param query The query string to be parsed.
     * @param schema The schema representing the data structure.
     */
    public QueryParser(String query, Schema schema) {
        // Tokenize the input query by spaces, parentheses, and special symbols
        this.schema = schema;
        tokens = tokenize(query);
    }
    /**
     * Tokenizes the input query string by splitting it into individual tokens based on
     * spaces, parentheses, commas, semicolons, and special characters. Each token is
     * stored as a string in a list.
     *
     * @param query The input query string to be tokenized.
     * @return A list of tokens extracted from the query string.
     */
    private List<String> tokenize(String query) {
        List<String> tokens = new ArrayList<>();
        StringBuilder token = new StringBuilder();
        for (char c : query.toCharArray()) {
            if (Character.isWhitespace(c)) {
                if (token.length() > 0) {
                    tokens.add(token.toString());
                    token.setLength(0);
                }
            } else if (c == ',' || c == '(' || c == ')' || c == ';') {
                if (token.length() > 0) {
                    tokens.add(token.toString());
                    token.setLength(0);
                }
                tokens.add(String.valueOf(c));
            } else {
                token.append(c);
            }
        }
        if (token.length() > 0) {
            tokens.add(token.toString());
        }
        return tokens;
    }


    /**
     * Parses the input query string by calling the appropriate methods to parse each
     * clause (FROM, DEFINE, PATTERN, WITHIN, RETURN) in the query. This method also
     * handles exceptions and logs error messages if parsing fails.
     *
     * @throws ParseException If there is a syntax error in the query.
     * @throws EBA.ParseException If there is an error while parsing PIE expressions.
     */
    public void parse() throws ParseException, EBA.ParseException {
//        parseSelectClause();
        try {
            parseFromClause();
            parseDefineClause();
            parsePatternClause();
            parseWithinClause();
            parseReturnClause();
        } catch (ParseException e) {
            // Capture the exception and log the error message
            logger.error("Exception caught: " + e.getMessage());
            // Rethrow the exception to ensure the program stops
            throw e;
        }
    }

    /**
     * Parses the 'RETURN' clause in the query, which defines the output specification.
     * The method adds the parsed output definitions to the returnClause list. It handles
     * multiple output definitions separated by commas.
     *
     * @throws ParseException If there is an error while parsing the 'RETURN' clause.
     */
    private void parseReturnClause() throws ParseException {
        expect("RETURN");
        do {
            returnClause.add(parseOutputDef());
            if (peek(",")) {
                consume(); // Consume comma
            } else {
                break;
            }
        } while (true);
    }

    /**
     * Parses an output definition in the query.
     * An output definition can either be a simple attribute reference (e.g., a.ts)
     * or an aggregate function (e.g., MAX(a.attr)).
     *
     * @return The parsed output definition as a string.
     * @throws ParseException If the output definition is invalid or cannot be parsed.
     */
    private String parseOutputDef() throws ParseException {
        String token = consume();
        if (token.matches("\\w+\\.\\w+")) {
            // Output definition like a.ts, s.te, etc.
            return token;
        } else if (token.matches("\\w+")) {
            // Aggregate function like MAX(a.attr)
            StringBuilder outputDef = new StringBuilder(token);
            outputDef.append(expect("("));
            outputDef.append(consume()); // Attribute name
            outputDef.append(expect(")"));
            return outputDef.toString();
        } else {
            throw new ParseException("Invalid output definition: " + token);
        }
    }

    /**
     * Parses the 'FROM' clause in the query, which specifies the input stream.
     *
     * @throws ParseException If the 'FROM' clause is not found or is malformed.
     */
    private void parseFromClause() throws ParseException {
        expect("FROM");
        fromClause = consume(); // Input stream name
    }

    /**
     * Parses the 'DEFINE' clause in the query, which defines the PIE (Predicate-Based Interval Event) expressions.
     * Multiple PIE expressions can be defined, separated by commas.
     *
     * @throws ParseException If there is an error while parsing the 'DEFINE' clause.
     * @throws EBA.ParseException If there is an error while parsing the PIE expressions.
     */
    private void parseDefineClause() throws ParseException, EBA.ParseException {
        expect("DEFINE");
        do {
            defineClause.add(parsePieDef());
            if (peek(",")) {
                consume(); // Consume comma
            } else {
                break;
            }
        } while (true);
    }

    /**
     * Parses a single PIE definition, which consists of an alias and a PIE expression.
     * The alias is mapped to the corresponding PIE expression.
     *
     * @return A map containing the alias and the corresponding PIE expression.
     * @throws ParseException If there is an error while parsing the alias or 'AS' keyword.
     * @throws EBA.ParseException If there is an error while parsing the PIE expression.
     */
    private Map<String, EBA> parsePieDef() throws ParseException, EBA.ParseException {
        String alias = consume(); // PIE alias
        expect("AS");
        EBA pie = parsePie();
        Map<String, EBA> pieMapping = new HashMap<>();
        pieMapping.put(alias, pie);
        EBA2String.put(pie, alias);
        return pieMapping;
    }

    /**
     * Parses a PIE (Predicate-Based Interval Event) expression.
     * This method supports simple predicates (e.g., accel > 8), complex expressions (e.g., (accel > 8)),
     * and negations (e.g., !accel > 8).
     *
     * @return The parsed PIE expression as an EBA object.
     * @throws ParseException If there is a syntax error in the expression.
     * @throws EBA.ParseException If there is an error in the PIE expression syntax.
     */
    private EBA parsePie() throws ParseException, EBA.ParseException {
        StringBuilder expressionBuilder = new StringBuilder();
        String token = consume();

        if (token.matches("\\w+")) {
            // Handling simple predicates, e.g., accel > 8
            expressionBuilder.append(token);
            if (peek(">") || peek("<") || peek(">=") || peek("<=") || peek("==") || peek("=") || peek("!=")) {
                expressionBuilder.append(" ").append(consume()); // append comparator
                expressionBuilder.append(" ").append(consume()); // append value
            }
        } else if (token.equals("(")) {
            // Handling complex expressions
            expressionBuilder.append("(");
            expressionBuilder.append(parsePie().toString());
            expressionBuilder.append(expect(")"));
        } else if (token.equals("!")) {
            expressionBuilder.append("!").append(parsePie().toString());
        } else {
            throw new EBA.ParseException("Invalid PIE: " + token);
        }

        // Use the EBAParser to parse the constructed expression
        return EBAParser.parse(expressionBuilder.toString(), schema);
    }

    /**
     * Parses the 'PATTERN' clause in the query, which defines temporal event patterns.
     * The pattern is parsed as a sequence of PIEs (Predicate-based Interval Events), which are optionally joined
     * by the AND operator. Each PIE is represented by an MPIEPairSource.
     *
     * @throws ParseException If there is an error while parsing the 'PATTERN' clause.
     * @throws EBA.ParseException If there is an error parsing the PIE expressions in the pattern.
     */
    private void parsePatternClause() throws ParseException, EBA.ParseException {
        expect("PATTERN");
        do {
            patternClause.add(parseMpiePairSource());
            if (peek("AND")) {
                JoinedNum++;
                consume(); // Consume AND and continue parsing
            } else {
                break;
            }
        } while (true);
    }

    /**
     * Parses a single MPIEPairSource from the pattern clause.
     * An MPIEPairSource consists of two PIEs (represented by their aliases) and the temporal relations between them.
     *
     * @return The parsed MPIEPairSource object.
     * @throws ParseException If there is an error while parsing the aliases or temporal relations.
     * @throws EBA.ParseException If there is an error while parsing the PIE expressions.
     */
    private MPIEPairSource parseMpiePairSource() throws ParseException, EBA.ParseException {
        String alias1 = consume(); // PIE alias 1
        EBA formerPred = getEbaByAlias(alias1); // Retrieve the first PIE's EBA
        predSetInPattern.add(formerPred);
        List<TemporalRelations.AllRel> relations = parseAllRelations(); // Parse the temporal relations

        String alias2 = consume(); // PIE alias 2
        EBA latterPred = getEbaByAlias(alias2); // Retrieve the second PIE's EBA
        predSetInPattern.add(latterPred);

        return new MPIEPairSource(relations, formerPred, latterPred);
    }

    /**
     * Retrieves the EBA (Event-Based Architecture) object associated with the specified alias.
     *
     * @param alias The alias of the PIE.
     * @return The EBA associated with the alias.
     * @throws ParseException If the alias is not defined in the 'DEFINE' clause.
     */
    private EBA getEbaByAlias(String alias) throws ParseException {
        for (Map<String, EBA> mapping : defineClause) {
            if (mapping.containsKey(alias)) {
                return mapping.get(alias);
            }
        }
        throw new ParseException("Undefined alias: " + alias);
    }

    /**
     * Parses a list of temporal relations in the 'DEFINE' or 'PATTERN' clauses.
     * Temporal relations define how different PIEs are related to each other in time.
     * The relations are consumed from the query until a semicolon or the end of the input.
     *
     * @return A list of TemporalRelations.PreciseRel objects representing the parsed relations.
     * @throws ParseException If there is an error while parsing the relations.
     */
    private List<TemporalRelations.PreciseRel> parseRelations() throws ParseException {
        List<TemporalRelations.PreciseRel> relations = new ArrayList<>();
        do {
            String relation = consume().toUpperCase();
            relation = relation.replace("-", "_");
            // Consume relation like "meets", "overlaps", etc.
            relations.add(TemporalRelations.AllRel.fromString(relation).getPreciseRel());
            if (peek(";")) {
                consume(); // Consume semicolon
            } else {
                break;
            }
        } while (true);
        return relations;
    }

    /**
     * Parses a list of all temporal relations in the 'DEFINE' or 'PATTERN' clauses.
     * This includes more general relations, which will be converted into precise relations later.
     *
     * @return A list of TemporalRelations.AllRel objects representing the parsed relations.
     * @throws ParseException If there is an error while parsing the relations.
     */
    private List<TemporalRelations.AllRel> parseAllRelations() throws ParseException {
        List<TemporalRelations.AllRel> relations = new ArrayList<>();
        do {
            String relation = consume().toUpperCase();
            relation = relation.replace("-", "_");
            // Consume relation like "meets", "overlaps", etc.
            relations.add(TemporalRelations.AllRel.fromString(relation));
            if (peek(";")) {
                consume(); // Consume semicolon
            } else {
                break;
            }
        } while (true);
        return relations;
    }

    /**
     * Parses the 'WITHIN' clause in the query, which defines the time window for the event pattern.
     * The time window is specified in units of seconds, minutes, or hours, and is converted to nanoseconds.
     *
     * @throws ParseException If the 'WITHIN' clause is not found or if the unit is invalid.
     */
    private void parseWithinClause() throws ParseException {
        expect("WITHIN");
        String number = consume();
        String unit = consume().toUpperCase();

        switch (unit) {
            case "H":
            case "HOUR":
            case "HOURS":
                withinClause = Long.parseLong(number) * 3600;
                break;
            case "M":
            case "MS":
            case "MIN":
            case "MINS":
            case "MINUTE":
            case "MINUTES":
                withinClause = Long.parseLong(number) * 60;
                break;
            case "S":
            case "SECS":
            case "SECONDE":
            case "SECONDES":
                withinClause = Long.parseLong(number);
                break;
            default:
                throw new ParseException("Invalid time unit: " + unit);
        }
        withinClause *= 1_000_000_000L; // Convert to nanoseconds
    }

    // Helper methods

    /**
     * Consumes the next token from the token list.
     *
     * @return The next token as a string.
     * @throws ParseException If there are no more tokens available.
     */
    private String consume() throws ParseException {
        if (currentIndex >= tokens.size()) {
            throw new ParseException("Unexpected end of input.");
        }
        return tokens.get(currentIndex++);
    }

    /**
     * Expects a specific token and consumes it if found.
     *
     * @param expectedToken The token expected in the input.
     * @return The consumed token if it matches the expected token.
     * @throws ParseException If the expected token is not found.
     */
    private String expect(String expectedToken) throws ParseException {
        String token = consume();
        if (!token.equals(expectedToken)) {
            throw new ParseException("Expected '" + expectedToken + "', but found '" + token + "'");
        }
        return token;
    }

    /**
     * Checks if the next token matches the expected token without consuming it.
     *
     * @param expectedToken The token expected in the input.
     * @return True if the next token matches the expected token, otherwise false.
     */
    private boolean peek(String expectedToken) {
        return currentIndex < tokens.size() && tokens.get(currentIndex).equals(expectedToken);
    }

    // Exception class for handling parsing errors

    /**
     * Custom exception for handling errors during the parsing process.
     */
    public static class ParseException extends Exception {
        public ParseException(String message) {
            super(message);
        }
    }

}
