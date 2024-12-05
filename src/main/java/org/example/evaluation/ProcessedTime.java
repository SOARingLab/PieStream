package org.example.evaluation;

import org.example.datasource.DataSource;
import org.example.datasource.FileDataSource;
import org.example.engine.Engine;
import org.example.engine.WindowType;
import org.example.events.Attribute;
import org.example.parser.Schema;

import java.io.IOException;
import java.util.*;

public class ProcessedTime {

    public static String buildSimpleJoinQuery(int Col, long windSize ) {
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
                    .append(" meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals ")
//                    .append(" meets;met-by;overlaps;started-by;during;finished-by;equals ")
                    .append("A").append(i + 1);
        }

        // 将所有部分组合成完整的查询语句
        String query = " SELECT A1.ts, A1.te" +
                "\n FROM dataStream" +
                "\n DEFINE " + defineBuilder.toString() +
                "\n PATTERN " + patternBuilder.toString() +
                "\n WINDOW "+windSize;

        return query;
    }


    public static Schema buildSchema(int Col) {
        List<Attribute> attriList = new ArrayList<>();
        for (int i = 1; i <= Col; i++) {
            attriList.add(new Attribute("a_" + i, "int"));
        }
        attriList.add(new Attribute("t1", "long"));
        attriList.add(new Attribute("t2", "long"));
        Schema schema= new Schema("CSV","t1", attriList);

        return schema;
    }

    public static long buildRunner(int col,  long limit, long windSize,String basePath, WindowType windowType ) {
        Schema schema =  buildSchema(col);
        String query =  buildSimpleJoinQuery(col,windSize); // Assuming buildQuery is used here

        Engine engine = new Engine(schema, query, windowType);
        StringBuilder dataPath = new StringBuilder();
        dataPath.append(basePath).append("events_col").append(col).append("_row").append(10000000).append(".csv");

        // Initialize FileDataSource and process data with the Engine
        try (DataSource dataSource = new FileDataSource(dataPath.toString(),limit)) {
            String line;
            long cnt = 0;
            long startTime = System.currentTimeMillis(); // Start timing
            while ((line = dataSource.readNext()) != null ) {
                engine.apply("", line); // Process each line of data
                cnt++;
//                if (cnt % 100000 == 0) {
//                    System.out.println(cnt + ", ");
//                }
            }
            long endTime = System.currentTimeMillis();

            System.out.println("\nTotal Lines Processed: " + (cnt));
            System.out.println("Processing time: " + (endTime - startTime) + " ms");
            engine.printResultCNT();
//            engine.printAccumulatedTimes();
            engine.printAVGprocessTime();
            return (endTime - startTime);

        } catch (IOException e) {
            System.err.println("Failed to open file: " + e.getMessage());
        }
        return 0;
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            execute(4, 100000, 100000, "/Users/czq/Code/TPS_data/");
//            System.err.println("xxx");
//            System.exit(1);
        }
        else{
            execute(Integer.valueOf(args[0]), Integer.valueOf(args[1]), Long.valueOf(args[2]), args[3]);
        }

//        execute(4, 100000, 100000, "/home/uzi/Code/TPSdata/");

    }

    private static void execute( int col,long limit, long windSize, String basePath) throws Exception {
        WindowType windowType = WindowType.TIME_WINDOW;
//        String basePath = "/home/uzi/Code/TPSdata/";
//        Map<Integer, Map<Long, Long>> col_row_proceTimeMap = new HashMap<>();
        System.out.println("===============  COL " + col + " ROW " + limit + " ===============");
        Long processedTime = buildRunner(col, limit, windSize, basePath, windowType);
//        System.out.println("processedTime: "+processedTime);
    }
}

// java  -Xms12g -Xmx12g  -cp "target/classes:lib/*" org.example.Main
//java  -Xms12g -Xmx12g   -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="test_service_jvmDump.hprof"  -cp "target/classes:lib/*" org.example.Main
// row = 1000000, window=100000
//{16=25558, 18=28040, 4=5680, 20=32368, 6=8413, 22=35275, 8=10719, 10=15273, 12=17323, 14=22095}


// col=4, window=100000
// row=10w， t=710  row=100w,t=5680  row=1000w,t=62799   row=10000w,t=  ，


//{128={100000=23662}, 64={100000=12946}, 32={100000=4916}, 16={100000=2255}, 4={100000=730}, 8={100000=1392}}

//win=1w,Mem=20G， {16={1000000=24180}, 32={1000000=48684}, 64={1000000=95697}, 128={1000000=208941}, 4={1000000=4301}, 8={1000000=10569}}
//win=10w,Mem=12G，OOM

// youhuahou
//(x) win=1w,Mem=12G,  {16={1000000=23032}, 32={1000000=50379}, 64={1000000=101087}, 128={1000000=215151}, 4={1000000=4242}, 8={1000000=10118}}
//(x) win=10w,Mem=12G, {16={1000000=22689}, 32={1000000=49815}, 64={1000000=93132}, 128={1000000=219770}, 4={1000000=4239}, 8={1000000=9969}}
// win=10w,Mem=12G, {128={100000=21406,10000000=2446517}}, {16={100000=2508, 10000000=275044}, 32={100000=5089, 10000000=570270}, 64={100000=10351, 10000000=1171065}, 4={100000=432, 10000000=50612}, 8={100000=1281, 10000000=123044}}
// {16={1000000=27415}, 32={1000000=55472}, 64={1000000=113639}, 128={1000000=239368}, 4={1000000=5386}, 8={1000000=12414}}
//

