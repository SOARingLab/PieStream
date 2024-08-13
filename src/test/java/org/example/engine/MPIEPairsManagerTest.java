package org.example.engine;

import org.example.events.Attribute;
import org.example.events.PointEvent;
import org.example.piepair.PIEPair;
import org.example.piepair.TemporalRelations;
import org.example.piepair.IEP;
import org.example.piepair.eba.EBA;
import org.example.piepair.eba.predicate.Greater;
import org.example.piepair.eba.predicate.Less;
import org.example.parser.MPIEPairSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MPIEPairsManagerTest {

    private MPIEPairsManager workerInstance;
    private PointEvent event1;
    private PointEvent event2;
    private PointEvent event3;
    private PointEvent event4;
    private PointEvent event5;
    private PointEvent event6;
    private EBA formerPred;
    private EBA latterPred;

    @BeforeEach
    public void setUp() {
        // 创建EBA实例
        Attribute attribute1 = new Attribute("speed", "int");
        formerPred = new EBA.PredicateEBA(new Greater(), attribute1, 50);

        Attribute attribute2 = new Attribute("acceleration", "float");
        latterPred = new EBA.PredicateEBA(new Less(), attribute2, 10.0);

        // 创建多个PointEvent实例
        // Z
        Map<Attribute, Object> payload1 = new HashMap<>();
        payload1.put(new Attribute("speed", "int"), 60);
        payload1.put(new Attribute("acceleration", "float"), 20.0);
        event1 = new PointEvent(payload1, System.currentTimeMillis());
        // E
        Map<Attribute, Object> payload2 = new HashMap<>();
        payload2.put(new Attribute("speed", "int"), 80);
        payload2.put(new Attribute("acceleration", "float"), 8.0);
        event2 = new PointEvent(payload2, System.currentTimeMillis());
        // I
        Map<Attribute, Object> payload3 = new HashMap<>();
        payload3.put(new Attribute("speed", "int"), 45);
        payload3.put(new Attribute("acceleration", "float"), 9.0);
        event3 = new PointEvent(payload3, System.currentTimeMillis());
        // O
        Map<Attribute, Object> payload4 = new HashMap<>();
        payload4.put(new Attribute("speed", "int"), 40);
        payload4.put(new Attribute("acceleration", "float"), 15.0);
        event4 = new PointEvent(payload4, System.currentTimeMillis());
        // E
        Map<Attribute, Object> payload5 = new HashMap<>();
        payload5.put(new Attribute("speed", "int"), 70);
        payload5.put(new Attribute("acceleration", "float"), 5.0);
        event5 = new PointEvent(payload5, System.currentTimeMillis());

        // I
        Map<Attribute, Object> payload6 = new HashMap<>();
        payload6.put(new Attribute("speed", "int"), 45);
        payload6.put(new Attribute("acceleration", "float"), 9.0);
        event6 = new PointEvent(payload6, System.currentTimeMillis());

        // 创建包含多个时间关系的 MPIEPairSource
        List<TemporalRelations.PreciseRel> relations = new ArrayList<>();
        relations.add(TemporalRelations.PreciseRel.OVERLAPS);
        relations.add(TemporalRelations.PreciseRel.STARTS);
        relations.add(TemporalRelations.PreciseRel.FINISHED_BY);

        MPIEPairSource source = new MPIEPairSource(relations, formerPred, latterPred);
        List<MPIEPairSource> sources = new ArrayList<>();
        sources.add(source);

        // 初始化Worker实例
        workerInstance = new MPIEPairsManager(sources,   50);
    }

    @Test
    public void testRunByPE_withMultipleEvents() {
        // 运行Worker的runByPE方法，依次传递多个事件
        workerInstance.runByPE(event1);
        workerInstance.runByPE(event2);
        workerInstance.runByPE(event3);
        workerInstance.runByPE(event4);
        workerInstance.runByPE(event5);
        workerInstance.runByPE(event6);

        workerInstance.printAllQueuesContents();
        // 检查PIEPairs是否正确处理了多个事件
        List<PIEPair> piePairs = workerInstance.getAllPiePairs();
        IEP fakeIEP =new IEP(TemporalRelations.PreciseRel.EQUALS,event6,event6,event6,event6,event6.getTimestamp(),event6.getTimestamp());
        piePairs.get(1).Q.enqueue(fakeIEP);
        workerInstance.getMPIEPairList().get(0).getQ().enqueue(fakeIEP);
        System.out.println("XXXXXXXXXXXXXXXXXXXXX ");
        workerInstance.printAllQueuesContents();

    }

}
