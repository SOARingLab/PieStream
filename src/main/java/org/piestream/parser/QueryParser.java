package org.piestream.parser;

import org.piestream.evaluation.Correct;
import org.piestream.piepair.TemporalRelations;
import org.piestream.piepair.eba.EBA;
import org.piestream.piepair.eba.EBAParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class QueryParser {

    private static final Logger logger = LoggerFactory.getLogger(QueryParser.class);
    private final List<String> tokens;
    private int currentIndex = 0;
    private Schema schema;

    private List<String> returnClause = new ArrayList<>();
    private String fromClause;
    private List<Map<String, EBA>> defineClause = new ArrayList<>();
    private Map<EBA,String> EBA2String =new HashMap<>();
    private List<MPIEPairSource> patternClause = new ArrayList<>();  // 更改为 MPIEPairSource 的 List
    private long withinClause =0;
    private int JoinedNum=0 ;
    private Set<EBA> predSetInPattern=new HashSet<EBA>();

    public List<MPIEPairSource> getPatternClause(){
        return  this.patternClause;
    }

    public Map<EBA,String> getEBA2String(){
        return  this.EBA2String;
    }

    public int getJoinedNum() {
        return JoinedNum;
    }

    public int getPredNumInPattern() {
        return predSetInPattern.size();
    }

    public long getwindowClause(){
        return  this.withinClause;
    }

    public QueryParser(String query, Schema schema) {
        // Tokenize the input query by spaces, parentheses, and special symbols
        this.schema=schema;
        tokens = tokenize(query);

    }

    // Tokenizer to split the query string into tokens
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

    // Entry point to parse the query
    public void parse() throws ParseException, EBA.ParseException {
//        parseSelectClause();
        try{
        parseFromClause();
        parseDefineClause();
        parsePatternClause();
        parseWithinClause();
        parseReturnClause();
        } catch (ParseException e) {
            // 捕获异常，输出异常信息
            logger.error("Exception caught: " + e.getMessage());
            // 你也可以重新抛出异常，确保程序中断
            throw e;
        }


    }

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

    private void parseFromClause() throws ParseException {
        expect("FROM");
        fromClause = consume(); // Input stream name
    }

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

    private Map<String, EBA> parsePieDef()  throws ParseException, EBA.ParseException {
        String alias = consume(); // PIE alias
        expect("AS");
        EBA pie = parsePie();
        Map<String, EBA> pieMapping = new HashMap<>();
        pieMapping.put(alias, pie);
        EBA2String.put(pie,alias);
        return pieMapping;
    }

    private EBA parsePie()  throws ParseException, EBA.ParseException {
        StringBuilder expressionBuilder = new StringBuilder();
        String token = consume();

        if (token.matches("\\w+")) {
            // 处理简单的谓词，例如 accel > 8
            expressionBuilder.append(token);
            if (peek(">") || peek("<") || peek(">=") || peek("<=") || peek("==")|| peek("=") || peek("!=")) {
                expressionBuilder.append(" ").append(consume()); // append comparator
                expressionBuilder.append(" ").append(consume()); // append value
            }
        } else if (token.equals("(")) {
            // 处理复杂的表达式
            expressionBuilder.append("(");
            expressionBuilder.append(parsePie().toString());
            expressionBuilder.append(expect(")"));
        } else if (token.equals("!")) {
            expressionBuilder.append("!").append(parsePie().toString());
        } else {
            throw new EBA.ParseException("Invalid PIE: " + token);
        }

        // 使用 EBAParser 解析构建的表达式
        return EBAParser.parse(expressionBuilder.toString(),schema);
    }

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

    private MPIEPairSource parseMpiePairSource() throws ParseException, EBA.ParseException {
        String alias1 = consume(); // PIE alias 1
        EBA formerPred = getEbaByAlias(alias1); // 获取第一个 PIE 的 EBA
        predSetInPattern.add(formerPred);
        List<TemporalRelations.AllRel> relations = parseAllRelations(); // 解析时间关系

        String alias2 = consume(); // PIE alias 2
        EBA latterPred = getEbaByAlias(alias2); // 获取第二个 PIE 的 EBA
        predSetInPattern.add(latterPred);

        return new MPIEPairSource(relations, formerPred, latterPred);
    }



    private EBA getEbaByAlias(String alias) throws ParseException {
        for (Map<String, EBA> mapping : defineClause) {
            if (mapping.containsKey(alias)) {
                return mapping.get(alias);
            }
        }
        throw new ParseException("Undefined alias: " + alias);
    }

    private List<TemporalRelations.PreciseRel> parseRelations() throws ParseException {
        List<TemporalRelations.PreciseRel> relations = new ArrayList<>();
        do {
            String relation = consume().toUpperCase();
            relation=relation.replace("-","_");
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
    private List<TemporalRelations.AllRel> parseAllRelations() throws ParseException {
        List<TemporalRelations.AllRel> relations = new ArrayList<>();
        do {
            String relation = consume().toUpperCase();
            relation=relation.replace("-","_");
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
        withinClause *= 1_000_000_000L; // 转换为纳秒
    }

    // Helper methods
    private String consume() throws ParseException {
        if (currentIndex >= tokens.size()) {
            throw new ParseException("Unexpected end of input.");
        }
        return tokens.get(currentIndex++);
    }

    private String expect(String expectedToken) throws ParseException {
        String token = consume();
        if (!token.equals(expectedToken)) {
            throw new ParseException("Expected '" + expectedToken + "', but found '" + token + "'");
        }
        return token;
    }

    private boolean peek(String expectedToken) {
        return currentIndex < tokens.size() && tokens.get(currentIndex).equals(expectedToken);
    }

    // Exception class for handling parsing errors
    public static class ParseException extends Exception {
        public ParseException(String message) {
            super(message);
        }
    }


}
