package org.piestream.evaluation;

import org.piestream.datasource.DataSource;
import org.piestream.datasource.FileDataSource;
import org.piestream.engine.Engine;
import org.piestream.engine.WindowType;
import org.piestream.events.Attribute;
import org.piestream.parser.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class Lowlatency {

    private static final Logger logger = LoggerFactory.getLogger(Lowlatency.class);
    public static String buildSimpleJoinQuery(int Col, long windSize ) {
        StringBuilder defineBuilder = new StringBuilder();
        StringBuilder patternBuilder = new StringBuilder();


        // 构建 DEFINE 部分
        for (int i = 1; i <= Col; i++) {
            if (i > 1) defineBuilder.append(", ");
            defineBuilder.append("A").append(i).append(" AS a_").append(i).append(" = 1 ");
        }

        String patternA4;
        // 构建 PATTERN 部分
        for (int i = 1; i < Col; i++) {
            if (i > 1) patternBuilder.append(" AND ");
            patternBuilder.append("A").append(i)
//                    .append(" meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals ")
//                    .append(" meets;met-by;overlaps;started-by;during;finished-by;equals ")
                    .append(" meets;overlaps;overlapped-by;starts;started-by;contains ")
                    .append("A").append(i + 1);
        }
//         patternA4=" A1 starts A2 AND A2 before A3 AND A3 overlaps A4";
        patternA4=patternBuilder.toString();
        // 将所有部分组合成完整的查询语句
        String query =
                " FROM dataStream " +
                "\n DEFINE " + defineBuilder.toString() +
                "\n PATTERN " + patternA4 +
                "\n WITHIN "+windSize + " S "+
                "\n RETURN A1.ts, A1.te " ;

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

    public static long buildRunner(int col,  long limit, long windSize,String basePath,long rate, WindowType windowType ) {
        Schema schema =  buildSchema(col);
        String query =  buildSimpleJoinQuery(col,windSize); // Assuming buildQuery is used here

        Engine engine = new Engine(schema, query, windowType);
        StringBuilder dataPath = new StringBuilder();
        dataPath.append(basePath).append("events_col").append(col).append("_row").append(10000000).append(".csv");

        // Initialize FileDataSource and process data with the Engine
        try (DataSource dataSource = new FileDataSource(dataPath.toString(),limit,rate)) {
            String line;
            long startTime = System.currentTimeMillis(); // Start timing
            while ((line = dataSource.readNext()) != null ) {
                engine.apply("", line); // Process each line of data

            }
            long endTime = System.currentTimeMillis();

            logger.info("\nTotal Lines Processed: " + (limit));
            logger.info("Processing time: " + (endTime - startTime) + " ms");
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
            logger.info("Runing Test : ");
            int col=4;
            long limit = 100000L;
            long windSize= 100000L;
            String dataPath= "/Users/czq/Code/TPS_data/";
//            String dataPath= "/home/uzi/Code/TPSdata/";
            long rate=100000;
            execute(col, limit, windSize, dataPath,rate);
        }
        else{
            execute(Integer.valueOf(args[0]), Integer.valueOf(args[1]), Long.valueOf(args[2]), args[3], Long.valueOf(args[4]));
        }
    }

    private static void execute( int col,long limit, long windSize, String basePath,long rate) throws Exception {
        WindowType windowType = WindowType.TIME_WINDOW;
        logger.info("=====>  COL " + col + ", LIMIT " + limit  + ", WINDSIZE " + windSize + ", DATAPATH " + basePath + ", RATE "   + rate +  ", <=====");
        Long processedTime = buildRunner(col, limit, windSize, basePath, rate,windowType);
    }
}
