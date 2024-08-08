package org.example.piepair;

import org.example.events.PointEvent;
import org.example.events.Schema;
import org.example.events.Attribute;
import org.example.events.CSVEventConverter;
import org.example.piepair.dfa.Alphabet;
import org.example.piepair.dfa.DFA;
import org.example.piepair.dfa.Dot2DFA;
import org.example.piepair.eba.EBA;
import org.example.piepair.eba.predicate.Equals;
import org.example.piepair.eba.predicate.Greater;
import org.example.piepair.eba.predicate.Predicate;
import org.example.piepair.TemporalRelations;

import java.util.ArrayList;
import java.util.List;

public class PIEPair {
    private final DFA dfa;
    private final EventClassifier classifier;
    private PointEvent formerPieStart;
    private PointEvent formerPieEnd;
    private PointEvent latterPieStart;
    private PointEvent latterPieEnd;

    public PIEPair(TemporalRelations.PreciseRel relation, EBA formerPred, EBA latterPred) {
        this.dfa = Dot2DFA.createDFAFromRelation(relation);
        this.classifier = new EventClassifier(formerPred, latterPred);
        this.formerPieStart = null;
        this.formerPieEnd = null;
        this.latterPieStart = null;
        this.latterPieEnd = null;
    }

    public void stepByPE(PointEvent event, Schema schema) {
        Alphabet alphabet = classifier.classify(event, schema);
        dfa.step(alphabet);
        recordEndPoint(event, alphabet);
    }

    private void recordEndPoint(PointEvent event, Alphabet alphabet) {
        if (dfa.isStateChanged()) {
            Alphabet lastAlphabet = dfa.getLastAlphabet();
            Alphabet currentAlphabet = dfa.getCurrentAlphabet();

            if ((lastAlphabet == Alphabet.O || lastAlphabet == Alphabet.I) && (currentAlphabet == Alphabet.Z || currentAlphabet == Alphabet.E)) {
                formerPieStart = event;
            }
            if ((lastAlphabet == Alphabet.Z || lastAlphabet == Alphabet.E) && (currentAlphabet == Alphabet.O || currentAlphabet == Alphabet.I)) {
                formerPieEnd = event;
            }
            if ((lastAlphabet == Alphabet.O || lastAlphabet == Alphabet.Z) && (currentAlphabet == Alphabet.I || currentAlphabet == Alphabet.E)) {
                latterPieStart = event;
            }
            if ((lastAlphabet == Alphabet.I || lastAlphabet == Alphabet.E) && (currentAlphabet == Alphabet.O || currentAlphabet == Alphabet.Z)) {
                latterPieEnd = event;
            }
        }
    }

    public boolean isFinal() {
        return dfa.isFinalState();
    }

    public boolean isTrigger() {
        return dfa.isTrigger();
    }

    public boolean isCompleted() {
        return dfa.isCompleted();
    }

    public boolean isStateChanged() {
        return dfa.isStateChanged();
    }

    public static void main(String[] args) {
        // 示例用法
        String csvFilePath = "src/main/resources/data/1m_linear_accel.csv"; // Replace with your CSV file path
        String schemaFilePath = "src/main/resources/domain/linear_accel.conf"; // Replace with your schema file path
        CSVEventConverter converter = new CSVEventConverter(csvFilePath, schemaFilePath);

        // 创建 Schema 实例
        Schema schema = new Schema(schemaFilePath);

        // 创建属性
        Attribute speedAttribute = new Attribute("SPEED");
        Attribute accelAttribute = new Attribute("ACCEL");

        // 创建参数
        Object speedParameter = "63";
        Object accelParameter = "0.005";

        // 使用谓词
        Predicate greater = new Greater();

        // 创建有效布尔表达式
        EBA formerExpression = new EBA.PredicateEBA(greater, speedAttribute, speedParameter);
        EBA latterExpression = new EBA.PredicateEBA(greater, accelAttribute, accelParameter);

        // 创建 PIEPair 实例
        PIEPair piePair = new PIEPair(TemporalRelations.PreciseRel.CONTAINS, formerExpression, latterExpression);

        // 遍历事件并进行处理
        List<PointEvent> triggeredEvents = new ArrayList<>();

        for (PointEvent event : converter) {
            piePair.stepByPE(event, schema);
            if (piePair.isTrigger()) {
                System.out.println("Triggered Event payload: " + event.getPayload());
                System.out.println("formerPieStart: " + (piePair.formerPieStart != null ? piePair.formerPieStart.getPayload() : "null"));
                System.out.println("formerPieEnd: " + (piePair.formerPieEnd != null ? piePair.formerPieEnd.getPayload() : "null"));
                System.out.println("latterPieStart: " + (piePair.latterPieStart != null ? piePair.latterPieStart.getPayload() : "null"));
                System.out.println("latterPieEnd: " + (piePair.latterPieEnd != null ? piePair.latterPieEnd.getPayload() : "null"));

            }
        }

    }
}
