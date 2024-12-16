package org.piestream.engine;

import org.apache.kafka.streams.kstream.ForeachAction;
//import org.apache.kafka.streams.kstream.Merger;
//import org.apache.kafka.streams.kstream.Windowed;
import org.piestream.events.PointEvent;
import org.piestream.merger.HashJoiner;
import org.piestream.merger.MapMerger;
import org.piestream.merger.Table;
import org.piestream.parser.Schema;
import org.piestream.events.Attribute;
import org.piestream.parser.MPIEPairSource;
import org.piestream.parser.QueryParser;
import org.piestream.piepair.eba.EBA;

import java.util.List;

public class Engine implements ForeachAction<String, String> {
    private final EventPreprocessor processor;  // 事件预处理器
    private final Attribute partitionAttribute; // 分区属性
    private final List<MPIEPairSource> MPPSourceList;  // 源列表
    private final Worker worker;  // Worker 实例
    private final QueryParser parser;  // 查询解析器


    // 构造函数，初始化相关属性
    public Engine(Schema schema, Attribute partitionAttribute, String query, WindowType winType) {
        this.parser = new QueryParser(query,schema);
        try {
            // 解析查询
            parser.parse();
        } catch (QueryParser.ParseException | EBA.ParseException e) {
            System.err.println("Failed to parse query: " + e.getMessage());
        }
        this.MPPSourceList = parser.getPatternClause();  // 获取解析后的模式子句
        long windowCapasityUnitNS=parser.getwindowClause();
        this.processor = new EventPreprocessor(schema);  // 初始化事件预处理器
        this.partitionAttribute = partitionAttribute;  // 设置分区属性
        Window window =new Window(winType,windowCapasityUnitNS,schema.getTimestampUnit().getNanosPerUnit());

        this.worker = new Worker(MPPSourceList,  window,parser.getEBA2String() );  // 创建 Worker 实例

    }

    public Engine(Schema schema,  String query){
         this(schema,null, query,WindowType.CAPACITY_WINDOW);
    }

    public Engine(Schema schema, String query, WindowType winType){
        this(schema,null, query,winType);
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
        worker.resetBeforeRun(pe.getTimestamp());
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
        long duration = preprocessTime+runOneByOneTime +deriveRelTime+mergeTime+updateTime;
        System.out.println("ALL Processing time: " + (double)duration/1000 + " s");
        System.out.println("Total preprocess time: " + (double)preprocessTime /1000 + " s");

        System.out.println("Total run one by one time: " + (double)runOneByOneTime/1000 + " s (" + Math.round ((double)runOneByOneTime/(double)duration *100)+"%");
        System.out.println("Total derive before-after relationship time: " + (double)deriveRelTime/1000 + " s (" + Math.round ((double)deriveRelTime/(double)duration *100)+"%");
        System.out.println("Total merge after run time: " + (double)mergeTime/1000 + " s (" + Math.round ((double)mergeTime/(double)duration *100)+"%");
        System.out.println("    refreshNewIepTable Time "+(double)this.worker.getTree().refreshNewIepTable /1000 + " s (" + Math.round ((double)this.worker.getTree().refreshNewIepTable/(double)duration *100)+"%");
        System.out.println("    Joined Time"+ (double)this.worker.getTree().joinTime/1000 + " s (" + Math.round ( (double)this.worker.getTree().joinTime/(double)duration *100)+"%");
        System.out.println("        searchForJoin Time: "+ (double)HashJoiner.searchForJoin /1000 + " s (" +Math.round ( (double)HashJoiner.searchForJoin/(double)duration *100)+"%");
//        System.out.println("    joinedCNT "+ HashJoiner.joinCNT +" times");
//        System.out.println("    concat Time"+ (double)this.worker.getTree().concat/1000 + " s (" +Math.round ( (double)this.worker.getTree().concat/(double)duration *100)+"%");
        System.out.println("Total update data time: " + (double)updateTime/1000 + " s (" +Math.round ( (double)updateTime/(double)duration *100)+"%");
//        System.out.println("    update_merged Time "+(double)this.worker.getTree().update_merged /1000 + " s (" + Math.round ((double)this.worker.getTree().update_merged/(double)duration *100)+"%");


        System.out.println("        concateTime Time "+ (double)Table.concateTime  /1000 + " s (" +Math.round ( (double)Table.concateTime  /(double)duration *100)+"%");
        System.out.println("            concateRebuilTime Time "+ (double)Table.concateRebuilTime  /1000 + " s (" +Math.round ( (double)Table.concateRebuilTime  /(double)duration *100)+"%");

        System.out.println("            addRowMergeTime Time "+ (double)Table.addRowMergeTime  /1000 + " s (" +Math.round ( (double)Table.addRowMergeTime  /(double)duration *100)+"%");
        System.out.println("                clearRowsTime Time "+ (double)Table.clearRowsTime  /1000 + " s (" +Math.round ( (double)Table.clearRowsTime  /(double)duration *100)+"%");

        System.out.println("                removeRowsAndIndexTime Time "+ (double)Table.removeRowsAndIndexTime  /1000 + " s (" +Math.round ( (double)Table.removeRowsAndIndexTime  /(double)duration *100)+"%");
        System.out.println("                addRowsTime Time "+ (double)Table.addRowsTime  /1000 + " s (" + Math.round ((double)Table.addRowsTime  /(double)duration *100)+"%");

        System.out.println("                merge_simple Time "+ (double)MapMerger.merge_simple /1000 + " s (" + Math.round ((double)MapMerger.merge_simple/(double)duration *100)+"%");
        System.out.println("    update_leaf Time "+(double)this.worker.getTree().update_leaf /1000 + " s (" +Math.round ( (double)this.worker.getTree().update_leaf/(double)duration *100)+"%");

    }



    public void printResultCNT(){
        worker.printResultCNT();
    }


    public void printAVGprocessTime(){
        System.out.println("AVG-latencyTime: "+worker.getTree().getRoot().getAVGprocessTime() +" ns");

    }



    public long getResultCNT(){
        return worker.getResultCNT();
    }


    public void formatResult(){
//        worker.printResultFormat();
        worker.printResultOrdered();
    }
}
