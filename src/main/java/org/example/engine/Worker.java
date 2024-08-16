package org.example.engine;

import org.example.events.PointEventIterator;
import org.example.events.Schema;
import org.example.events.PointEvent;
import org.example.piepair.IEP;
import org.example.piepair.PIEPair;
import org.example.parser.MPIEPairSource;
import org.example.utils.CircularQueue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import java.util.List;

public class Worker {

    private final MPIEPairsManager mpiEPairsManager;
    private final List<PIEPair> AllPiePairs;
    private final ExecutorService executor;

    public Worker(List<MPIEPairSource> MPPSourceList,  int QCapacity) {
        this.mpiEPairsManager =new MPIEPairsManager(MPPSourceList,QCapacity)  ;
        this.AllPiePairs= mpiEPairsManager.getAllPiePairs();
        // 初始化 ExecutorService，线程池大小可以根据需求调整
        this.executor = Executors.newFixedThreadPool(AllPiePairs.size());
    }

    // 获取 MPIEPairsManager
    public MPIEPairsManager getMpiEPairsManager() {
        return mpiEPairsManager;
    }


    // 执行事件处理，使用 MPIEPairsManager 来处理事件
    public void processEvent(Object event) {
        // 根据 Schema 预处理事件（假设你有 EventPreprocessor 类可以用来处理事件）
        // PointEvent processedEvent = eventPreprocessor.preprocess(event);

        // 这里假设直接处理传入的事件
        mpiEPairsManager.runByPE((PointEvent) event);
    }



    /**
     * 打印 mpiEPairsManager 中的每个 MPIEPair 及其对应的 CircularQueue (Q) 的内容。
     */
    public void printQ() {
        mpiEPairsManager.printAllQueuesContents();
    }

    public void runOneByOne(PointEvent pe) {
        AllPiePairs.forEach(pair -> pair.stepByPE(pe));
    }

    // 并发执行 stepByPE 方法
//    public void run(PointEvent pe) {
//        AllPiePairs.forEach(piePair -> executor.submit(() -> piePair.stepByPE(pe)));
//    }

    public void run(  PointEvent pe) {

        for (int i = 0; i < AllPiePairs.size(); i++) {
            final PIEPair piePair = AllPiePairs.get(i);

            executor.submit(() -> {
                piePair.stepByPE(pe);
            });
        }
    }

    public void run(  PointEventIterator peItr) {



    }

    // 在应用程序结束时，记得关闭 ExecutorService
    public void shutdown() {
        executor.shutdown();
        // 如果需要立即停止所有正在执行的任务，可以使用 executor.shutdownNow();
    }
}
