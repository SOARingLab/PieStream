package org.example.piepair.eba;

import org.example.events.Attribute;
import org.example.events.PointEvent;
import org.example.events.Schema;
import org.example.events.CSVEventConverter;

import org.example.piepair.eba.predicate.Predicate;
import org.example.piepair.eba.predicate.Equals;
import org.example.piepair.eba.predicate.Greater;

import java.util.ArrayList;
import java.util.List;

public abstract class EBA {

    public abstract boolean evaluate(PointEvent event, Schema schema);

    public static class PredicateEBA extends EBA {
        private final Predicate predicate;
        private final Attribute attribute;
        private final Object parameter;

        public PredicateEBA(Predicate predicate, Attribute attribute, Object parameter) {
            this.predicate = predicate;
            this.attribute = attribute;
            this.parameter = parameter;
        }

        @Override
        public boolean evaluate(PointEvent event, Schema schema) {
            return predicate.test(event, attribute, parameter, schema);
        }
    }

    public static class AndEBA extends EBA {
        private final List<EBA> expressions;

        public AndEBA(EBA... expressions) {
            this.expressions = new ArrayList<>();
            for (EBA expression : expressions) {
                this.expressions.add(expression);
            }
        }

        @Override
        public boolean evaluate(PointEvent event, Schema schema) {
            for (EBA expression : expressions) {
                if (!expression.evaluate(event, schema)) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class OrEBA extends EBA {
        private final List<EBA> expressions;

        public OrEBA(EBA... expressions) {
            this.expressions = new ArrayList<>();
            for (EBA expression : expressions) {
                this.expressions.add(expression);
            }
        }

        @Override
        public boolean evaluate(PointEvent event, Schema schema) {
            for (EBA expression : expressions) {
                if (expression.evaluate(event, schema)) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class NotEBA extends EBA {
        private final EBA expression;

        public NotEBA(EBA expression) {
            this.expression = expression;
        }

        @Override
        public boolean evaluate(PointEvent event, Schema schema) {
            return !expression.evaluate(event, schema);
        }

    }

    public static void main(String[] args) {
        // Create the CSV event converter
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
        EBA equalsExpression = new EBA.PredicateEBA(equals, speedAttribute, equalsParameter);
        EBA greaterExpression = new EBA.PredicateEBA(greater, accelAttribute, greaterParameter);

        // 创建组合表达式
        EBA andExpression = new EBA.AndEBA(equalsExpression, greaterExpression);
        EBA orExpression = new EBA.OrEBA(equalsExpression, greaterExpression);
        EBA notExpression = new EBA.NotEBA(equalsExpression);

        // 遍历事件并应用表达式
        int count = 0;
        for (PointEvent event : converter) {
            if (count >= 20) break;

            boolean andResult = andExpression.evaluate(event, schema);
            boolean orResult = orExpression.evaluate(event, schema);
            boolean notResult = notExpression.evaluate(event, schema);

            if (  orResult  ) {
                System.out.println("Event payload: " + event.getPayload());
                System.out.println("AND result: " + andResult);
                System.out.println("OR result: " + orResult);
                System.out.println("NOT result: " + notResult);
                count++;
            }
        }
    }
}