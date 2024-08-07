package org.example.piepair;

import org.example.events.PointEvent;
import org.example.events.Schema;
import org.example.events.Attribute;
import org.example.events.CSVEventConverter;
import org.example.piepair.eba.EBA;
import org.example.piepair.eba.predicate.Equals;
import org.example.piepair.eba.predicate.Greater;
import org.example.piepair.eba.predicate.Predicate;
import org.example.piepair.dfa.Alphabet;

public class EventClassifier {
    private final EBA formerPred;
    private final EBA latterPred;

    public EventClassifier(EBA formerPred, EBA latterPred) {
        this.formerPred = formerPred;
        this.latterPred = latterPred;
    }

    public Alphabet classify(PointEvent event, Schema schema) {
        boolean formerResult = formerPred.evaluate(event, schema);
        boolean latterResult = latterPred.evaluate(event, schema);

        if (formerResult && latterResult) {
            return Alphabet.E; // Both FormerPred and LatterPred are true
        } else if (!formerResult && !latterResult) {
            return Alphabet.O; // Both FormerPred and LatterPred are false
        } else if (formerResult && !latterResult) {
            return Alphabet.Z; // FormerPred is true, LatterPred is false
        } else {
            return Alphabet.I; // FormerPred is false, LatterPred is true
        }
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
        Object equalsParameter = "32";
        Object greaterParameter = "1.5";

        // 使用谓词
        Predicate equals = new Equals();
        Predicate greater = new Greater();

        // 创建有效布尔表达式
        EBA formerExpression = new EBA.PredicateEBA(equals, speedAttribute, equalsParameter);
        EBA latterExpression = new EBA.PredicateEBA(greater, accelAttribute, greaterParameter);

        // 创建 EventClassifier 实例
        EventClassifier classifier = new EventClassifier(formerExpression, latterExpression);

        // 遍历事件并进行分类
        int count = 0;
        for (PointEvent event : converter) {
            if (count >= 20) break;
            Alphabet classification = classifier.classify(event, schema);
            System.out.println("Event payload: " + event.getPayload());
            System.out.println("Classification: " + classification);
            count++;
        }
    }
}
