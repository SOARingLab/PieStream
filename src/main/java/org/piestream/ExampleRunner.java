package org.piestream;

import org.piestream.datasource.DataSource;
import org.piestream.datasource.FileDataSource;
import org.piestream.engine.Engine;
import org.piestream.engine.WindowType;
import org.piestream.events.Attribute;
import org.piestream.parser.Schema;

import java.io.IOException;
import java.util.*;

public class ExampleRunner {

    public static String buildSimpleJoinQuery(int Col ) {
        StringBuilder defineBuilder = new StringBuilder();
        StringBuilder patternBuilder = new StringBuilder();

        // 构建 DEFINE 部分
        for (int i = 1; i <= Col; i++) {
            if (i > 1) defineBuilder.append(", ");
            defineBuilder.append("A").append(i).append(" AS a_").append(i).append(" = 1 ");
        }

        // 构建 PATTERN 部分
        for (int i = 1; i < Col; i++) {
            if (i > 1) patternBuilder.append(" AND ");
            patternBuilder.append("A").append(i)
//                    .append(" meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals ")
                    .append(" meets;met-by;overlaps;started-by;during;finished-by;equals ")
                    .append("A").append(i + 1);
        }

        // 将所有部分组合成完整的查询语句
        String query = " FROM dataStream" +
                "\n DEFINE " + defineBuilder.toString() +
                "\n PATTERN " + patternBuilder.toString() +
                "\n WITHIN 100000 S"+
                "\n RETURN A1.ts, A1.te ";

        return query;
    }

    public static Schema buildSchema(int Col) {
        List<Attribute> attriList = new ArrayList<>();
        for (int i = 1; i <= Col; i++) {
            attriList.add(new Attribute("a_" + i, "int"));
        }
        attriList.add(new Attribute("ts","long"));
        attriList.add(new Attribute("te","long"));
        return new Schema("CSV","ts",  attriList);
    }

    public static long buildRunner(int col, long limit, String basePath, WindowType windowType ) {
        Schema schema = ExampleRunner.buildSchema(col);
        String query = ExampleRunner.buildSimpleJoinQuery(col ); // Assuming buildQuery is used here

        Engine engine = new Engine(schema, query, windowType);
        StringBuilder dataPath = new StringBuilder();
        dataPath.append(basePath).append("events_col").append(col).append("_row10000000").append(".csv");

        // Initialize FileDataSource and process data with the Engine
        try (DataSource dataSource = new FileDataSource(dataPath.toString(),limit)) {
            String line;
            long startTime = System.currentTimeMillis(); // Start timing
            while ((line = dataSource.readNext()) != null ) {
                engine.apply("", line); // Process each line of data
            }
            long endTime = System.currentTimeMillis();

            System.out.println("\nTotal Lines Processed: " + (limit));
            System.out.println("Processing time: " + (endTime - startTime) + " ms");
            engine.printResultCNT();
            engine.printAccumulatedTimes();
            return  (endTime - startTime);

        } catch (IOException e) {
            System.err.println("Failed to open file: " + e.getMessage());
        }
        return 0;
    }



    public static void main(String[] args) {

        List<Integer> colList = new ArrayList<>(Arrays.asList(4 ));
        List<Long> limitList = new ArrayList<>(Arrays.asList( 10_000_000L ));
        WindowType windowType=WindowType.TIME_WINDOW;
        String basePath = "/home/uzi/Code/TPSdata/";

//        String basePath = "/Users/czq/Code/TPS_data/";
        Map<Integer, Map<Long, Long>> col_row_proceTimeMap = new HashMap<>();

        for (int col : colList) {
            // 使用 computeIfAbsent 保证每个 col 对应一个新的 Map<Long, Long>
            col_row_proceTimeMap.computeIfAbsent(col, k -> new HashMap<>());

            for (long limit : limitList) {
                System.out.println("===============  COL "+col+" LIMIT "+limit+" ===============");
                Long processedTime = buildRunner(col, limit, basePath, windowType );
                col_row_proceTimeMap.get(col).put(limit, processedTime);
                System.out.println(col_row_proceTimeMap);
            }
        }

    }
}

