package org.example.engine;

import org.example.parser.MPIEPairSource;
import org.example.piepair.PIEPair;
import org.example.piepair.IEP;
import org.example.utils.CircularQueue;
import org.example.events.PointEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MPIEPairsManager {
    private final List<MPIEPairSource> MPPSourceList;
    private final int QCapacity;
    private final List<MPIEPair> MPIEPairList;
    private final Map<MPIEPairSource, MPIEPair> MPPSourceToPairMap;  // MPPSource到MPIEPair的映射
    private final Map<MPIEPairSource, CircularQueue<IEP>> MPPSourceToQueueMap;  // MPPSource到Q的映射
    private final List<PIEPair> AllPiePairs;  // 新增的AllPiePairs成员


    public MPIEPairsManager(List<MPIEPairSource> MPPSourceList , int QCapacity) {
        this.MPPSourceList = MPPSourceList;
        this.QCapacity = QCapacity;
        this.MPIEPairList = new ArrayList<>();
        this.MPPSourceToPairMap = new HashMap<>();  // 初始化MPIEPair映射
        this.MPPSourceToQueueMap = new HashMap<>();  // 初始化Queue映射
        this.AllPiePairs = new ArrayList<>();  // 初始化AllPiePairs

        for (MPIEPairSource MPPSource : MPPSourceList) {
            MPIEPair mpiePair = new MPIEPair(MPPSource.getRelations(), MPPSource.getFormerPred(), MPPSource.getLatterPred(), QCapacity);
            this.MPIEPairList.add(mpiePair);
            this.MPPSourceToPairMap.put(MPPSource, mpiePair);  // 将 MPIEPairSource 和对应的 MPIEPair 放入映射中
            this.MPPSourceToQueueMap.put(MPPSource, mpiePair.getQ());  // 将 MPIEPairSource 和对应的 Queue 放入映射中
            this.AllPiePairs.addAll(mpiePair.getPiePairs());  // 将 MPIEPair 中的所有 PIEPair 添加到 AllPiePairs 中
        }
    }

    // 获取 MPPSourceList
    public List<MPIEPairSource> getMPPSourceList() {
        return MPPSourceList;
    }

    // 获取 QCapacity
    public int getQCapacity() {
        return QCapacity;
    }

    // 获取 MPIEPairList
    public List<MPIEPair> getMPIEPairList() {
        return MPIEPairList;
    }

    // 获取 MPPSource 到 MPIEPair 的映射
    public Map<MPIEPairSource, MPIEPair> getMPPSourceToPairMap() {
        return MPPSourceToPairMap;
    }

    // 获取 MPPSource 到 Queue 的映射
    public Map<MPIEPairSource, CircularQueue<IEP>> getMPPSourceToQueueMap() {
        return MPPSourceToQueueMap;
    }

    // 获取 AllPiePairs
    public List<PIEPair> getAllPiePairs() {
        return AllPiePairs;
    }

    // 根据 MPIEPairSource 获取对应的 MPIEPair
    public MPIEPair getMPIEPairBySource(MPIEPairSource source) {
        return MPPSourceToPairMap.get(source);
    }

    // 根据 MPIEPairSource 获取对应的 Queue
    public CircularQueue<IEP> getQueueBySource(MPIEPairSource source) {
        return MPPSourceToQueueMap.get(source);
    }

//    // 并发运行每个 PIEPair 的 stepByPE 方法
//    public void runByPEConcurrently(PointEvent event) {
//        for (PIEPair piePair : AllPiePairs) {
//            executorService.submit(() -> piePair.stepByPE(event));
//        }
//    }

    public void runByPE(PointEvent event) {
        for (PIEPair piePair : AllPiePairs) {
            piePair.stepByPE(event);  // 串行执行
        }
    }

    /**
     * 输出 MPPSourceToQueueMap 中所有 CircularQueue<IEP> 中的 IEP。
     */
    public void printAllQueuesContents() {
        for (Map.Entry<MPIEPairSource, CircularQueue<IEP>> entry : MPPSourceToQueueMap.entrySet()) {
            MPIEPairSource source = entry.getKey();
            CircularQueue<IEP> queue = entry.getValue();

            System.out.println("MPIEPairSource: " + source);
            System.out.println("CircularQueue Contents:");

            queue.printQueue();

            System.out.println("-----------------------------");
        }
    }

//    // 关闭线程池
//    public void shutdown() {
//        executorService.shutdown();
//    }
}
