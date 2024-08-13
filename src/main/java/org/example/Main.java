package org.example;

import org.example.datasource.KafkaStreamProcessor;
import org.example.engine.Engine;
import org.example.engine.MPIEPairsManager;
import org.example.events.Attribute;
import org.example.events.Schema;
import org.example.parser.MPIEPairSource;
import org.example.piepair.TemporalRelations;

import org.example.piepair.eba.EBA;
import org.example.piepair.eba.predicate.Greater;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        // Kafka Stream 配置参数
        String bootstrapServers = "localhost:9092"; // Kafka broker 地址
        String applicationId = "kafka-stream-prodscessdas"; // Kafka Stream 应用 ID
        String inputTopic = "linear000"; // 输入的 Kafka 主题

        String schemaFilePath = "src/main/resources/domain/linear_accel.yaml"; // Replace with your config file path
        Schema schema=new Schema(schemaFilePath);

        // 创建EBA实例
        Attribute attribute1 = new Attribute("SPEED", "int");
        EBA formerPred = new EBA.PredicateEBA(new Greater(), attribute1, 30);

        Attribute attribute2 = new Attribute("Lane", "int");
        EBA latterPred = new EBA.PredicateEBA(new Greater(), attribute2, 2);


        // 创建包含多个时间关系的 MPIEPairSource
        List<TemporalRelations.PreciseRel> relations = new ArrayList<>();
        relations.add(TemporalRelations.PreciseRel.OVERLAPS);
        relations.add(TemporalRelations.PreciseRel.STARTS);
        relations.add(TemporalRelations.PreciseRel.FINISHED_BY);
        relations.add(TemporalRelations.PreciseRel.OVERLAPPED_BY);
        relations.add(TemporalRelations.PreciseRel.DURING);
        relations.add(TemporalRelations.PreciseRel.CONTAINS);

        MPIEPairSource source = new MPIEPairSource(relations, formerPred, latterPred);
        List<MPIEPairSource> sources = new ArrayList<>();
        sources.add(source);

        // 创建 Engine 实例
        Engine engine = new Engine(sources,schema,"", 50);

        // 创建 Kafka Stream Processor 实例，并传入 Engine 作为处理函数
        KafkaStreamProcessor processor = new KafkaStreamProcessor(bootstrapServers, applicationId, inputTopic, engine);

        // 启动 Kafka Stream 处理
        processor.start();

        // 添加 JVM 关闭钩子，在应用程序终止时关闭 Kafka Stream 处理
        processor.addShutdownHook();
    }
}