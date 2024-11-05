package org.example.utils;

import org.example.datasource.DataSource;
import org.example.datasource.FileDataSource;
import org.example.engine.Engine;
import org.example.engine.WindowType;
import org.example.events.Attribute;
import org.example.parser.Schema;

import java.io.IOException;
import java.util.*;

public class ExampleBuilder {

    public static String buildSimpleJoinQuery(int Col) {
        StringBuilder defineBuilder = new StringBuilder();
        StringBuilder patternBuilder = new StringBuilder();

        // 构建 DEFINE 部分
        for (int i = 1; i <= Col; i++) {
            if (i > 1) defineBuilder.append(", ");
            defineBuilder.append("A").append(i).append(" AS a_").append(i).append(" > 0");
        }

        // 构建 PATTERN 部分
        for (int i = 1; i < Col; i++) {
            if (i > 1) patternBuilder.append(" AND ");
            patternBuilder.append("A").append(i)
                    .append(" meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals ")
                    .append("A").append(i + 1);
        }

        // 将所有部分组合成完整的查询语句
        String query = " SELECT A1.ts, A1.te" +
                "\n FROM dataStream" +
                "\n DEFINE " + defineBuilder.toString() +
                "\n PATTERN " + patternBuilder.toString() +
                "\n WINDOW 100000";

        return query;
    }

    public static Schema buildSchema(int Col) {
        List<Attribute> attriList = new ArrayList<>();
        for (int i = 1; i <= Col; i++) {
            attriList.add(new Attribute("a_" + i, "int"));
        }

        return new Schema("CSV",  attriList);
    }

    public static long buildRunner(int col, int row, String basePath, WindowType windowType) {
        Schema schema = ExampleBuilder.buildSchema(col);
        String query = ExampleBuilder.buildSimpleJoinQuery(col); // Assuming buildQuery is used here

        Engine engine = new Engine(schema, query, windowType);
        StringBuilder dataPath = new StringBuilder();
        dataPath.append(basePath).append("col").append(col).append("_row").append(row).append(".csv");

        // Initialize FileDataSource and process data with the Engine
        try (DataSource dataSource = new FileDataSource(dataPath.toString())) {
            String line;
            long cnt = 1;
            long startTime = System.currentTimeMillis(); // Start timing
            while ((line = dataSource.readNext()) != null) {
                engine.apply("", line); // Process each line of data
                cnt++;
                if (cnt % 100000 == 0) {
                    System.out.println(cnt + ", ");
                }
            }
            long endTime = System.currentTimeMillis();

            System.out.println("\nTotal Lines Processed: " + (cnt-1));
            System.out.println("Processing time: " + (endTime - startTime) + " ms");
            engine.printResultCNT();
//            engine.printAccumulatedTimes();
            return  (endTime - startTime);

        } catch (IOException e) {
            System.err.println("Failed to open file: " + e.getMessage());
        }
        return 0;
    }


    public static void main(String[] args) {

        List<Integer> colList = new ArrayList<>(Arrays.asList(4, 6, 8, 10, 12, 14, 16, 18, 20, 22));
        int row = 1000000;
        String basePath = "/Users/czq/Code/TPstream0/TPStream_DAPD/jepc-v2/";
        Map<Integer,Long> processedTimeMap=new HashMap<>();
        // Run with TIME_WINDOW
        for(int col :colList){
            processedTimeMap.put(col,buildRunner(col, row, basePath, WindowType.TIME_WINDOW));
        }

    }
}
