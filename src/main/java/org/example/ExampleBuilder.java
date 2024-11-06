package org.example;

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
                    .append(" meets;met-by;overlaps;started-by;during;finished-by;equals ")
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

    public static long buildRunner(int col, long row, String basePath, WindowType windowType) {
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
                if (cnt % 10000000 == 0) {
                    System.out.println(cnt + ", ");
                }
            }
            long endTime = System.currentTimeMillis();

            System.out.println("\nTotal Lines Processed: " + (cnt-1));
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

//        List<Integer> colList = new ArrayList<>(Arrays.asList(4, 6, 8, 10, 12, 14, 16, 18, 20, 22));
        List<Integer> colList = new ArrayList<>(Arrays.asList(4, 6));
//        List<Integer> colList = new ArrayList<>(Arrays.asList( 4));
        List<Long> rowList = new ArrayList<>(Arrays.asList(100_000L, 1000_000L, 10_000_000L, 100_000_000L));

        WindowType windowType=WindowType.TIME_WINDOW;
//        String basePath = "/Users/czq/Code/TPstream0/TPStream_DAPD/jepc-v2/";

        String basePath = "/home/uzi/Code/TPS/jepc-v2/";
        Map<Integer, Map<Long, Long>> col_row_proceTimeMap = new HashMap<>();

        for (int col : colList) {
            // 使用 computeIfAbsent 保证每个 col 对应一个新的 Map<Long, Long>
            col_row_proceTimeMap.computeIfAbsent(col, k -> new HashMap<>());

            for (long row : rowList) {
                // 调用 buildRunner 方法，获取处理时间并存入 map
                Long processedTime = buildRunner(col, row, basePath, windowType);

                // 把结果放入嵌套的 Map 中
                col_row_proceTimeMap.get(col).put(row, processedTime);

                // 打印当前处理后的 Map
                System.out.println(col_row_proceTimeMap);
            }
        }

        System.out.println(col_row_proceTimeMap);
    }
}

// row = 1000000, window=100000
//{16=25558, 18=28040, 4=5680, 20=32368, 6=8413, 22=35275, 8=10719, 10=15273, 12=17323, 14=22095}


// col=4, window=100000
// row=10w， t=710  row=100w,t=5680  row=1000w,t=62799   row=10000w,t=  ，