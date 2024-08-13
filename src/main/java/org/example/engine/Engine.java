package org.example.engine;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.example.events.PointEvent;
import org.example.events.Schema;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.parser.MPIEPairSource;

import java.util.List;
import java.util.Map;

public class Engine implements ForeachAction<String, String> {

    private final Worker worker; // 添加 EventPreprocessor 成员


    // 构造函数，传入 EventPreprocessor 实例
    public Engine(List<MPIEPairSource> MPPSourceList, Schema schema, String partitionValue, int QCapacity) {

        this.worker = new Worker (MPPSourceList,schema,partitionValue,QCapacity) ;
    }

    @Override
    public void apply(String key, String value) {
        // 处理每条记录的逻辑
        System.out.println("Processing record with key: " + key + ", value: " + value);
        worker.run(value);
        worker.printQ();
    }
}
