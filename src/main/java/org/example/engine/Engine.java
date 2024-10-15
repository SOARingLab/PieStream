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

    // 处理 Kafka 记录
    @Override
    public void apply(String key, String value) {
        // 预处理，解析元数据
        PointEvent pe = processor.preprocess(value);

        // 逐条处理事件
        worker.resetBeforeRun();
        worker.runOneByOne(pe);
        worker.deriveBeforeAfterRel();
        worker.mergeAfterRun();
        worker.updateData();
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
