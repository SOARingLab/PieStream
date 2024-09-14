//package org.example.engine;
//
//import org.example.events.Attribute;
//import org.example.piepair.IEP;
//import org.example.piepair.PIEPair;
//import org.example.piepair.TemporalRelations;
//import org.example.piepair.eba.EBA;
//import org.example.events.PointEvent;
//import org.example.piepair.predicate.Greater;
//import org.example.piepair.predicate.Less;
//import org.example.utils.CircularQueue;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//class MPIEPairTest {
//    private PointEvent event1;
//    private PointEvent event2;
//    private PointEvent event3;
//    private PointEvent event4;
//    private PointEvent event5;
//    private EBA formerPred;
//    private EBA latterPred;
//    private List<TemporalRelations.PreciseRel> relations;
//    private MPIEPair mpiEPair;
//
//    @BeforeEach
//    void setUp() {
//        // 创建EBA实例
//        Attribute attribute1 = new Attribute("speed", "int");
//        formerPred = new EBA.PredicateEBA(new Greater(), attribute1, 50);
//
//        Attribute attribute2 = new Attribute("acceleration", "float");
//        latterPred = new EBA.PredicateEBA(new Less(), attribute2, 10.0);
//
//
//        // 创建符合former条件的PointEvent
//        Map<Attribute, Object> payload1 = new HashMap<>();
//        payload1.put(new Attribute("speed", "int"), 60);
//        payload1.put(new Attribute("acceleration", "float"), 20.0);
//        event1 = new PointEvent(payload1, System.currentTimeMillis());
//
//        Map<Attribute, Object> payload2 = new HashMap<>();
//        payload2.put(new Attribute("speed", "int"), 80);
//        payload2.put(new Attribute("acceleration", "float"), 8.0);
//        event2 = new PointEvent(payload2, System.currentTimeMillis());
//
//        Map<Attribute, Object> payload3 = new HashMap<>();
//        payload3.put(new Attribute("speed", "int"), 10);
//        payload3.put(new Attribute("acceleration", "float"), 8.0);
//        event3 = new PointEvent(payload3, System.currentTimeMillis());
//
//        event4 = new PointEvent(payload3, System.currentTimeMillis());
//        event5 = new PointEvent(payload1, System.currentTimeMillis());
//
//
//        relations = new ArrayList<>();
//        relations.add(TemporalRelations.PreciseRel.OVERLAPS);
//        relations.add(TemporalRelations.PreciseRel.STARTS);
//        relations.add(TemporalRelations.PreciseRel.DURING);
//
//        // 初始化 MPIEPair
//        mpiEPair = new MPIEPair(relations, formerPred, latterPred, 30);
//    }
//
//    @Test
//    void testConstructorAndGetPiePairs() {
//        // 验证 PIEPair 列表的大小是否与 PreciseRel 列表的大小相同
//        assertEquals(relations.size(), mpiEPair.getPiePairs().size(), "PIEPair list size should match relations size");
//
//        // 验证每个 PIEPair 的初始化是否正确
//        for (int i = 0; i < relations.size(); i++) {
//            PIEPair piePair = mpiEPair.getPiePairs().get(i);
//            assertNotNull(piePair, "PIEPair should not be null");
//            assertEquals(relations.get(i), piePair.getRelation(), "PIEPair relation should match the expected relation");
//        }
//    }
//
//    @Test
//    void testCircularQueueInitialization() {
//        // 验证 CircularQueue 是否被正确初始化
//        CircularQueue<IEP> queue = mpiEPair.getQ();
//        assertNotNull(queue, "CircularQueue should not be null");
//        assertEquals(0, queue.size(), "CircularQueue should be empty initially");
//        assertEquals(30, queue.capacity(), "CircularQueue capacity should match the specified capacity");
//    }
//
//    // 其他可能的测试
//    @Test
//    void testPiePairFunctionality() {
//        // 创建线程池，线程数量与 piePairs 的数量相同
//        ExecutorService executorService = Executors.newFixedThreadPool(mpiEPair.getPiePairs().size());
//
//        // 使用 CountDownLatch 来等待所有线程完成
//        CountDownLatch latch = new CountDownLatch(mpiEPair.getPiePairs().size());
//
//        // 对每个 PIEPair 创建并发任务
//        for (PIEPair piePair : mpiEPair.getPiePairs()) {
//            executorService.submit(() -> {
//                try {
//                    // 执行测试操作
//                    assertDoesNotThrow(() -> piePair.stepByPE(event1), "Processing event1 should not throw an exception");
//                    assertDoesNotThrow(() -> piePair.stepByPE(event2), "Processing event2 should not throw an exception");
//                } finally {
//                    // 确保任务完成时减少 CountDownLatch 计数
//                    latch.countDown();
//                }
//            });
//        }
//
//        try {
//            // 等待所有线程完成
//            latch.await();
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            fail("Test was interrupted");
//        } finally {
//            // 关闭线程池
//            executorService.shutdown();
//        }
//
//
//
//    }
//}
