//package org.example.engine;
//
//import org.example.datasource.DataSource;
//import org.example.events.Attribute;
//import org.example.events.PointEventIterator;
//import org.example.events.PointEvent;
//import org.example.events.Schema;
//import org.example.parser.MPIEPairSource;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.List;
//import java.util.ArrayList;
//
//public class PartitionFilter {
//
//    // 成员变量：用于存储 partitionValue 和对应 Worker 的映射
//    private final Map<String, Worker> partitionMap;
//    private final Map<String, PointEventIterator> partitionedStreams;
//
//
//    // 构造函数：初始化 partitionMap
//    public PartitionFilter() {
//        this.partitionMap = new HashMap<>();
//        this.partitionedStreams = new HashMap<>();
//    }
//
//
//    // 添加一个 Worker 实例到映射中
//    public void addWorker(String partitionValue, Worker worker) {
//        partitionMap.put(partitionValue, worker);
//    }
//
//    // 根据 partitionValue 获取相应的 Worker
//    public Worker getWorker(String partitionValue) {
//        return partitionMap.get(partitionValue);
//    }
//
//    // 移除一个特定 partitionValue 的 Worker
//    public void removeWorker(String partitionValue) {
//        partitionMap.remove(partitionValue);
//    }
//
//    // 判断是否存在指定 partitionValue 的 Worker
//    public boolean containsPartition(String partitionValue) {
//        return partitionMap.containsKey(partitionValue);
//    }
//
//    public List<Worker> getAllWorkers() {
//        return new ArrayList<>(partitionMap.values());
//    }
//
//    // 读取并预处理数据
//    public void processDataStream(List<MPIEPairSource> MPPSourceList,   int QCapacity ,DataSource dataSource, EventPreprocessor  preprocessoror, Attribute partitionAttribute) {
//        while (dataSource.hasNext()) {
//            String rawEvent = dataSource.readNext();
//            PointEvent pe = preprocessoror.preprocess(rawEvent);
//
//
//            //partition worker
//            String partitionValue=String.valueOf (pe.getPayload().get(partitionAttribute));
//
//            Worker worker = partitionMap.get(partitionValue);
//            if ( worker == null){
//                worker=new Worker(MPPSourceList,QCapacity);
//                addWorker(partitionValue,worker );
//            }
//
//            //partition stream
//            PointEventIterator iterator = partitionedStreams.get(partitionValue);
//
//            if (iterator == null) {
//                iterator = new PointEventIterator();
//                partitionedStreams.put(partitionValue, iterator);
//            }
//
//            iterator.addEvent(pe);
//        }
//    }
//
//
//}
