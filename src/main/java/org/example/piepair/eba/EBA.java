package org.example.piepair.eba;

import org.example.events.Attribute;
import org.example.events.PointEvent;
import org.example.piepair.eba.predicate.Predicate;

import java.util.ArrayList;
import java.util.List;

public abstract class EBA {

    public abstract boolean evaluate(PointEvent event);

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
        public boolean evaluate(PointEvent event) {
            return predicate.test(event, attribute, parameter);
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
        public boolean evaluate(PointEvent event) {
            for (EBA expression : expressions) {
                if (!expression.evaluate(event)) {
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
        public boolean evaluate(PointEvent event) {
            for (EBA expression : expressions) {
                if (expression.evaluate(event)) {
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
        public boolean evaluate(PointEvent event) {
            return !expression.evaluate(event);
        }

    }

}
