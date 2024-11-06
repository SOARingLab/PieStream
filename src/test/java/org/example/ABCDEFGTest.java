
package org.example;

import org.example.datasource.DataSource;
import org.example.datasource.FileDataSource;
import org.example.engine.Engine;
import org.example.engine.WindowType;
import org.example.parser.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ABCDEFGTest {

    @Test
    public void testEngineProcessing() {


        int Col=7;
        int Row=1000000;
        Schema schema =  ExampleBuilder.buildSchema(Col);
        String query= ExampleBuilder.buildSimpleJoinQuery(Col);

        Engine engine = new Engine(schema, query,WindowType.TIME_WINDOW);
//        Engine engine = new Engine(schema, query,WindowType.COUNT_WINDOW);
        String basePath="/Users/czq/Code/TPstream0/TPStream_DAPD/jepc-v2/";
        StringBuilder dataPath = new StringBuilder();
        dataPath.append(basePath).append("col").append(Col).append("_row").append(Row).append(".csv");
        // File data source, read data and apply to Engine
        try (DataSource dataSource = new FileDataSource(dataPath.toString())) {
            String line;
            long cnt=1;
            long lastResCNT=0;
            long resCNT=0;
            long startTime = System.currentTimeMillis(); // 开始计时
            while ((line = dataSource.readNext()) != null) {
//                System.out.println("Line Read: " + line);
                engine.apply("", line); // Process each line of data
                resCNT = engine.getResultCNT();
//                if(resCNT != lastResCNT){
//
//                    System.out.println( " +  "+ (resCNT - lastResCNT)+ " |  "+ "Time : "+( cnt )+" Results : " + resCNT  );
//                    lastResCNT =  resCNT;
////                    System.out.println(cnt);
//                    engine.formatResult();
//                }
                cnt++;
                if(cnt%100000==0){
                    System.out.println( cnt+", ");
                }

            }
//            engine.formatResult();
            System.out.println( "\nCNT:"+cnt);

            engine.printResultCNT();

            long endTime = System.currentTimeMillis(); // 结束计时
            long duration = endTime - startTime; // 计算持续时间
            System.out.println("Processing time: " + duration + " ms");

            engine.printAccumulatedTimes();

        } catch (IOException e) {
            System.err.println("Failed to open file: " + e.getMessage());
        }

    }
}
