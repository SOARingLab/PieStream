package org.example.engine;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.example.events.PointEvent;
import org.example.parser.Schema;
import org.example.events.Attribute;
import org.example.parser.MPIEPairSource;
import org.example.parser.QueryParser;
import org.example.piepair.eba.EBA;

import java.util.List;

public class Engine implements ForeachAction<String, String> {
    private final EventPreprocessor processor;  // 事件预处理器
    private final Attribute partitionAttribute; // 分区属性
    private final List<MPIEPairSource> MPPSourceList;  // 源列表
    private final Worker worker;  // Worker 实例
    private final QueryParser parser;  // 查询解析器


    // 构造函数，初始化相关属性
    public Engine(Schema schema, Attribute partitionAttribute, String query) {
        this.parser = new QueryParser(query,schema);
        try {
            // 解析查询
            parser.parse();
        } catch (QueryParser.ParseException | EBA.ParseException e) {
            System.err.println("Failed to parse query: " + e.getMessage());
        }
        this.MPPSourceList = parser.getPatternClause();  // 获取解析后的模式子句
        long QCapacity=parser.getwindowClause();
        this.processor = new EventPreprocessor(schema);  // 初始化事件预处理器
        this.partitionAttribute = partitionAttribute;  // 设置分区属性

        this.worker = new Worker(MPPSourceList, QCapacity,parser.getEBA2String() );  // 创建 Worker 实例
    }

    public Engine(Schema schema,  String query){
         this(schema,null, query);
    }

//    // 处理 Kafka 记录
//    @Override
//    public void apply(String key, String value) {
//        // 预处理，解析元数据
//        PointEvent pe = processor.preprocess(value);
//
//        // 逐条处理事件
//        worker.resetBeforeRun();
//        worker.runOneByOne(pe);
//        worker.deriveBeforeAfterRel();
//        worker.mergeAfterRun();
//        worker.updateData();
//    }

    private long preprocessTime = 0;
    private long runOneByOneTime = 0;
    private long deriveRelTime = 0;
    private long mergeTime = 0;
    private long updateTime = 0;

    public void apply(String key, String value) {
        long startTime, endTime;

        // 预处理，解析元数据
        startTime = System.currentTimeMillis();
        PointEvent pe = processor.preprocess(value);
        endTime = System.currentTimeMillis();
        preprocessTime += (endTime - startTime);

        // 逐条处理事件
        startTime = System.currentTimeMillis();
        worker.resetBeforeRun();
        worker.runOneByOne(pe);
        endTime = System.currentTimeMillis();
        runOneByOneTime += (endTime - startTime);

        // 处理前后关系
        startTime = System.currentTimeMillis();
        worker.deriveBeforeAfterRel();
        endTime = System.currentTimeMillis();
        deriveRelTime += (endTime - startTime);

        // 合并操作
        startTime = System.currentTimeMillis();
        worker.mergeAfterRun();
        endTime = System.currentTimeMillis();
        mergeTime += (endTime - startTime);

        // 更新数据
        startTime = System.currentTimeMillis();
        worker.updateData();
        endTime = System.currentTimeMillis();
        updateTime += (endTime - startTime);
    }

    // 可以在适当的时候调用这个方法来输出累积时间
    public void printAccumulatedTimes() {
        System.out.println("Total preprocess time: " + preprocessTime + " ms");
        System.out.println("Total run one by one time: " + runOneByOneTime + " ms");
        System.out.println("Total derive before-after relationship time: " + deriveRelTime + " ms");
        System.out.println("Total merge after run time: " + mergeTime + " ms");
        System.out.println("Total update data time: " + updateTime + " ms");
    }



    public void printResultCNT(){
        worker.printResultCNT();
    }


    public long getResultCNT(){
        return worker.getResultCNT();
    }


    public void formatResult(){
//        worker.printResultFormat();
        worker.printResultOrdered();
    }
}
