package org.example.engine;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.example.datasource.DataSource;
import org.example.events.PointEvent;
import org.example.events.PointEventIterator;
import org.example.events.Schema;
import org.example.events.Attribute;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.parser.MPIEPairSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Engine implements ForeachAction<String, String> {
    private final EventPreprocessor processor;
    private final Attribute partitionAttribute;
    private final List<MPIEPairSource> MPPSourceList;
    private final int QCapacity;
    private Worker worker;



    // 构造函数，传入 EventPreprocessor 实例
    public Engine(List<MPIEPairSource> MPPSourceList, Schema schema,  Attribute  partitionAttribute , int QCapacity) {

        this.MPPSourceList=MPPSourceList;
        this.processor=new EventPreprocessor(schema);
        this.partitionAttribute=partitionAttribute;
        this.QCapacity=QCapacity;
        this.worker=new Worker(MPPSourceList,QCapacity);
    }

    @Override
    public void apply(String key, String value) {
        // 处理每条记录的逻辑
//        System.out.println("Processing record with key: " + key + ", value: " + value);
        // 预处理，解析元数据
        PointEvent pe=processor.preprocess(value);

//        //partition
//        String partitionValue=String.valueOf (pe.getPayload().get(partitionAttribute));

        //run
        worker.runOneByOne(pe);
//        worker.run(pe);
        worker.printQ();

    }

    public String run( String value) {
        // 处理每条记录的逻辑
        System.out.println(  "  " + value);
        // 预处理，解析元数据
        PointEvent pe=processor.preprocess(value);

//        //partition
//        String partitionValue=String.valueOf (pe.getPayload().get(partitionAttribute));


        Worker worker=new Worker(MPPSourceList,QCapacity);
        //run
        worker.runOneByOne(pe);
//        worker.run(pe);
        worker.printQ();
        return  "";
    }


//    public void runFromSource( DataSource dataS) {
//          // 预处理，解析元数据
//        PointEventIterator peItr=processor.preprocessSource(dataS);
//
//        //partition
//        String partitionValue=String.valueOf (pe.getPayload().get(partitionAttribute));
//
//        Worker worker=new Worker(MPPSourceList,QCapacity);
//        //run
//        worker.run(peItr);
//        worker.printQ();
//    }

    public void runFromSource(DataSource dataS) {
        // 创建一个线程池，固定大小为2个线程
        ExecutorService executor = Executors.newFixedThreadPool(1);

        // 定义一个预处理任务
        Runnable preprocessingTask = () -> {
            // 预处理数据源，获取预处理后的 PointEventIterator
            PointEventIterator peItr = processor.preprocessSource(dataS);

            // 提交数据处理任务到线程池
            executor.submit(() -> {
                // 创建 Worker 实例
                Worker worker = new Worker(MPPSourceList, QCapacity);
                // 使用 Worker 处理预处理后的 PointEventIterator
                worker.run(peItr);
                // 打印队列内容
                worker.printQ();
            });
        };

        // 启动一个新线程来执行预处理任务
        new Thread(preprocessingTask).start();
    }

//
//    public void shutdown() {
//        for (Worker worker : filter.getAllWorkers()) {
//            worker.shutdown();
//        }
//    }
}
