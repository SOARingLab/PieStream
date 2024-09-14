//package org.example.piepair.eba;
//import org.example.piepair.eba.EBA;
//import org.example.piepair.predicate.Equals;
//import org.example.piepair.predicate.Greater;
//import org.example.piepair.predicate.Less;
//import org.example.events.Attribute;
//import org.example.events.PointEvent;
//import org.junit.jupiter.api.Test;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import static org.junit.jupiter.api.Assertions.assertTrue;
//import static org.junit.jupiter.api.Assertions.assertFalse;
//
//public class EBATest {
//
//    @Test
//    public void testSimpleAndEBA() {
//        // 创建 PointEvent 和 Attribute
//        Map<Attribute, Object> payload = new HashMap<>();
//        Attribute speedAttr = new Attribute("SPEED", "int");
//        Attribute vidAttr = new Attribute("VID", "int");
//        payload.put(speedAttr, 60);
//        payload.put(vidAttr, 123);
//        PointEvent event = new PointEvent(payload, System.currentTimeMillis());
//
//        // 创建 PredicateEBA
//        EBA equalsEBA = new EBA.PredicateEBA(new Equals(), vidAttr, 123);
//        EBA greaterEBA = new EBA.PredicateEBA(new Greater(), speedAttr, 50);
//
//        // 创建 AndEBA
//        EBA andEBA = new EBA.AndEBA(equalsEBA, greaterEBA);
//
//        // 评估表达式并断言结果
//        assertTrue(andEBA.evaluate(event), "The evaluation should be true for the provided PointEvent");
//    }
//
//    @Test
//    public void testSimpleOrEBA() {
//        // 创建 PointEvent 和 Attribute
//        Map<Attribute, Object> payload = new HashMap<>();
//        Attribute speedAttr = new Attribute("SPEED", "int");
//        Attribute vidAttr = new Attribute("VID", "int");
//        payload.put(speedAttr, 40);  // 改变 SPEED 值以触发 OR 逻辑
//        payload.put(vidAttr, 123);
//        PointEvent event = new PointEvent(payload, System.currentTimeMillis());
//
//        // 创建 PredicateEBA
//        EBA equalsEBA = new EBA.PredicateEBA(new Equals(), vidAttr, 123);
//        EBA greaterEBA = new EBA.PredicateEBA(new Greater(), speedAttr, 50);
//
//        // 创建 OrEBA
//        EBA orEBA = new EBA.OrEBA(equalsEBA, greaterEBA);
//
//        // 评估表达式并断言结果
//        assertTrue(orEBA.evaluate(event), "The evaluation should be true since one of the conditions is true");
//    }
//
//    @Test
//    public void testComplexEBA() {
//        // 创建 PointEvent 和 Attribute
//        Map<Attribute, Object> payload = new HashMap<>();
//        Attribute speedAttr = new Attribute("SPEED", "int");
//        Attribute vidAttr = new Attribute("VID", "int");
//        Attribute tempAttr = new Attribute("TEMP", "double");
//        payload.put(speedAttr, 60);
//        payload.put(vidAttr, 123);
//        payload.put(tempAttr, 25.0);
//        PointEvent event = new PointEvent(payload, System.currentTimeMillis());
//
//        // 创建 PredicateEBA
//        EBA equalsEBA = new EBA.PredicateEBA(new Equals(), vidAttr, 123);
//        EBA greaterEBA = new EBA.PredicateEBA(new Greater(), speedAttr, 50);
//        EBA lessEBA = new EBA.PredicateEBA(new Less(), tempAttr, 30.0);
//
//        // 创建复杂的 EBA
//        EBA andEBA = new EBA.AndEBA(equalsEBA, greaterEBA); // (VID == 123) AND (SPEED > 50)
//        EBA complexEBA = new EBA.OrEBA(andEBA, lessEBA); // ((VID == 123) AND (SPEED > 50)) OR (TEMP < 30.0)
//
//        // 评估表达式并断言结果
//        assertTrue(complexEBA.evaluate(event), "The evaluation should be true for the provided PointEvent");
//    }
//
//    @Test
//    public void testNestedAndOrEBA() {
//        // 创建 PointEvent 和 Attribute
//        Map<Attribute, Object> payload = new HashMap<>();
//        Attribute speedAttr = new Attribute("SPEED", "int");
//        Attribute vidAttr = new Attribute("VID", "int");
//        Attribute tempAttr = new Attribute("TEMP", "double");
//        payload.put(speedAttr, 40);  // 改变 SPEED 值以触发复杂的 OR/AND 逻辑
//        payload.put(vidAttr, 123);
//        payload.put(tempAttr, 35.0); // 改变 TEMP 值以触发复杂的 OR/AND 逻辑
//        PointEvent event = new PointEvent(payload, System.currentTimeMillis());
//
//        // 创建 PredicateEBA
//        EBA equalsEBA = new EBA.PredicateEBA(new Equals(), vidAttr, 123);
//        EBA greaterEBA = new EBA.PredicateEBA(new Greater(), speedAttr, 50);
//        EBA lessEBA = new EBA.PredicateEBA(new Less(), tempAttr, 30.0);
//
//        // 创建多层的 EBA
//        EBA andEBA = new EBA.AndEBA(equalsEBA, greaterEBA); // (VID == 123) AND (SPEED > 50)
//        EBA orEBA = new EBA.OrEBA(andEBA, lessEBA); // ((VID == 123) AND (SPEED > 50)) OR (TEMP < 30.0)
//        EBA notEBA = new EBA.NotEBA(orEBA); // NOT(((VID == 123) AND (SPEED > 50)) OR (TEMP < 30.0))
//
//        // 评估表达式并断言结果
//        assertFalse(!notEBA.evaluate(event), "The evaluation should be true for the provided PointEvent");
//    }
//
//    @Test
//    public void testComplexNestedEBA() {
//        // 创建 PointEvent 和 Attribute
//        Map<Attribute, Object> payload = new HashMap<>();
//        Attribute speedAttr = new Attribute("SPEED", "int");
//        Attribute vidAttr = new Attribute("VID", "int");
//        Attribute tempAttr = new Attribute("TEMP", "double");
//        Attribute pressureAttr = new Attribute("PRESSURE", "double");
//        payload.put(speedAttr, 60);
//        payload.put(vidAttr, 123);
//        payload.put(tempAttr, 25.0);
//        payload.put(pressureAttr, 100.0);
//        PointEvent event = new PointEvent(payload, System.currentTimeMillis());
//
//        // 创建 PredicateEBA
//        EBA equalsEBA = new EBA.PredicateEBA(new Equals(), vidAttr, 123);
//        EBA greaterEBA = new EBA.PredicateEBA(new Greater(), speedAttr, 50);
//        EBA lessTempEBA = new EBA.PredicateEBA(new Less(), tempAttr, 30.0);
//        EBA greaterPressureEBA = new EBA.PredicateEBA(new Greater(), pressureAttr, 90.0);
//
//        // 创建复杂的 EBA
//        EBA andEBA = new EBA.AndEBA(equalsEBA, greaterEBA); // (VID == 123) AND (SPEED > 50)
//        EBA orEBA = new EBA.OrEBA(andEBA, lessTempEBA); // ((VID == 123) AND (SPEED > 50)) OR (TEMP < 30.0)
//        EBA complexNestedEBA = new EBA.AndEBA(orEBA, greaterPressureEBA); // (((VID == 123) AND (SPEED > 50)) OR (TEMP < 30.0)) AND (PRESSURE > 90.0)
//
//        // 评估表达式并断言结果
//        assertTrue(complexNestedEBA.evaluate(event), "The evaluation should be true for the provided PointEvent");
//    }
//}
