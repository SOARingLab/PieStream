package org.example.piepair;

import org.example.events.Attribute;
import org.example.events.PointEvent;
import org.example.piepair.dfa.Alphabet;
import org.example.piepair.dfa.DFA;
import org.example.piepair.eba.EBA;
import org.example.piepair.eba.predicate.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class PIEPairTest {

    private PIEPair piePair;
    private PointEvent event1;
    private PointEvent event2;
    private PointEvent event3;
    private PointEvent event4;
    private PointEvent event5;
    private EBA formerPred;
    private EBA latterPred;
    @BeforeEach
    public void setUp() {
        // 创建EBA实例
        Attribute attribute1 = new Attribute("speed", "int");
        formerPred = new EBA.PredicateEBA(new Greater(), attribute1, 50);

        Attribute attribute2 = new Attribute("acceleration", "float");
        latterPred = new EBA.PredicateEBA(new Less(), attribute2, 10.0);


        // 创建符合former条件的PointEvent
        Map<Attribute, Object> payload1 = new HashMap<>();
        payload1.put(new Attribute("speed", "int"), 60);
        payload1.put(new Attribute("acceleration", "float"), 20.0);
        event1 = new PointEvent(payload1, System.currentTimeMillis());

        Map<Attribute, Object> payload2 = new HashMap<>();
        payload2.put(new Attribute("speed", "int"), 80);
        payload2.put(new Attribute("acceleration", "float"), 8.0);
        event2 = new PointEvent(payload2, System.currentTimeMillis());

        Map<Attribute, Object> payload3 = new HashMap<>();
        payload3.put(new Attribute("speed", "int"), 10);
        payload3.put(new Attribute("acceleration", "float"), 8.0);
        event3 = new PointEvent(payload3, System.currentTimeMillis());

        event4 = new PointEvent(payload3, System.currentTimeMillis());
        event5 = new PointEvent(payload1, System.currentTimeMillis());
    }

    @Test
    public void testStepByPE_withPE1() {
        // 初始化PIEPair实例
        piePair = new PIEPair(TemporalRelations.PreciseRel.OVERLAPS, formerPred, latterPred);

        // 进行状态转换
        Alphabet alp1=piePair.stepByPE(event1);

        // 检查formerPieStart是否被正确记录
        assertEquals(alp1,Alphabet.Z);

        assertNull(piePair.getFormerPieEnd());
        assertEquals(event1, piePair.getFormerPieStart());

        assertNull(piePair.getLatterPieEnd());
        assertNull(piePair.getLatterPieStart());

        assertFalse(piePair.isTrigger());
        assertFalse(piePair.isFinal());
        assertFalse(piePair.isCompleted());
        assertTrue(piePair.isStateChanged());
    }

    @Test
    public void testStepByPE_withPE2() {
        // 初始化PIEPair实例
        piePair = new PIEPair(TemporalRelations.PreciseRel.OVERLAPS, formerPred, latterPred);

        // 进行状态转换
        Alphabet alp1=piePair.stepByPE(event1);
        assertEquals(alp1,Alphabet.Z);
        Alphabet alp2=piePair.stepByPE(event2);

        // 检查latterPieEnd是否被正确记录
        assertEquals(alp2,Alphabet.E);

        assertNull(piePair.getFormerPieEnd());
        assertEquals(event1, piePair.getFormerPieStart());

        assertNull(piePair.getLatterPieEnd());
        assertEquals(event2, piePair.getLatterPieStart());

        assertFalse(piePair.isTrigger());
        assertFalse(piePair.isFinal());
        assertFalse(piePair.isCompleted());
        assertTrue(piePair.isStateChanged());
    }

    @Test
    public void testStepByPE_withPE3() {
        // 初始化PIEPair实例
        piePair = new PIEPair(TemporalRelations.PreciseRel.OVERLAPS, formerPred, latterPred);

        // 进行状态转换
        Alphabet alp1=piePair.stepByPE(event1);
        Alphabet alp2=piePair.stepByPE(event2);
        Alphabet alp3=piePair.stepByPE(event3);

        // 检查latterPieEnd是否被正确记录
        assertEquals(alp3,Alphabet.I);

        assertEquals(event1, piePair.getFormerPieStart());
        assertEquals(event3, piePair.getFormerPieEnd());

        assertEquals(event2, piePair.getLatterPieStart());
        assertNull(piePair.getLatterPieEnd());

        assertTrue(piePair.isTrigger());
        assertTrue(piePair.isFinal());
        assertFalse(piePair.isCompleted());
        assertTrue(piePair.isStateChanged());
    }
    @Test
    public void testStepByPE_withPE4() {
        // 初始化PIEPair实例
        piePair = new PIEPair(TemporalRelations.PreciseRel.OVERLAPS, formerPred, latterPred);

        // 进行状态转换
        Alphabet alp1=piePair.stepByPE(event1);
        Alphabet alp2=piePair.stepByPE(event2);
        Alphabet alp3=piePair.stepByPE(event3);
        Alphabet alp4=piePair.stepByPE(event4);

        // 检查latterPieEnd是否被正确记录
        assertEquals(alp3,Alphabet.I);

        assertEquals(event1, piePair.getFormerPieStart());
        assertEquals(event3, piePair.getFormerPieEnd());

        assertEquals(event2, piePair.getLatterPieStart());
        assertNull(piePair.getLatterPieEnd());

        assertFalse(piePair.isTrigger());
        assertTrue(piePair.isFinal());
        assertFalse(piePair.isCompleted());
        assertFalse(piePair.isStateChanged());
    }
    @Test
    public void testStepByPE_withPE5() {
        // 初始化PIEPair实例
        piePair = new PIEPair(TemporalRelations.PreciseRel.OVERLAPS, formerPred, latterPred);

        // 进行状态转换
        Alphabet alp1=piePair.stepByPE(event1);
        Alphabet alp2=piePair.stepByPE(event2);
        Alphabet alp3=piePair.stepByPE(event3);
        Alphabet alp4=piePair.stepByPE(event4);
        Alphabet alp5=piePair.stepByPE(event5);

        // 检查latterPieEnd是否被正确记录
        assertEquals(alp5,Alphabet.Z);

        assertEquals(event5, piePair.getFormerPieStart());
        assertEquals(event3, piePair.getFormerPieEnd());

        assertEquals(event2, piePair.getLatterPieStart());
        assertEquals(event5, piePair.getLatterPieEnd());

        assertFalse(piePair.isTrigger());
        assertFalse(piePair.isFinal());
        assertTrue(piePair.isCompleted());
        assertTrue(piePair.isStateChanged());
    }


}
