package org.example.engine;

import org.example.events.Schema;
import org.example.events.PointEvent;
import org.example.piepair.PIEPair;
import org.example.parser.MPIEPairSource;

import java.util.List;

public class Worker {

    private final MPIEPairsManager mpiEPairsManager;
    private final Schema schema;
    private final String partitionValue;
    private final EventPreprocessor processor;
    private final List<PIEPair> AllPiePairs;

    public Worker(List<MPIEPairSource> MPPSourceList, Schema schema, String partitionValue, int QCapacity) {
        this.mpiEPairsManager =new MPIEPairsManager(MPPSourceList,QCapacity)  ;
        this.schema = schema;
        this.partitionValue = partitionValue;
        this.processor=new EventPreprocessor(schema);
        this.AllPiePairs= mpiEPairsManager.getAllPiePairs();
    }

    // 获取 MPIEPairsManager
    public MPIEPairsManager getMpiEPairsManager() {
        return mpiEPairsManager;
    }

    // 获取 Schema
    public Schema getSchema() {
        return schema;
    }

    // 获取 partitionValue
    public String getPartitionValue() {
        return partitionValue;
    }

    // 执行事件处理，使用 MPIEPairsManager 来处理事件
    public void processEvent(Object event) {
        // 根据 Schema 预处理事件（假设你有 EventPreprocessor 类可以用来处理事件）
        // PointEvent processedEvent = eventPreprocessor.preprocess(event);

        // 这里假设直接处理传入的事件
        mpiEPairsManager.runByPE((PointEvent) event);
    }


    public void run(Object rawdata) {
        AllPiePairs.forEach(pair -> pair.stepByPE(processor.preprocess(rawdata)));
    }

    /**
     * 打印 mpiEPairsManager 中的每个 MPIEPair 及其对应的 CircularQueue (Q) 的内容。
     */
    public void printQ() {
        mpiEPairsManager.getMPIEPairList().forEach(mpiEPair -> {
            System.out.println("\n\nMPIEPair: \n" + mpiEPair);
            System.out.println("Q (CircularQueue): \n"  );
            mpiEPair.getQ().printQueue();
        });
    }

}
