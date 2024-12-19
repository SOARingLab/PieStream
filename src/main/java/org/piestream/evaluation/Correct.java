package org.piestream.evaluation;

import org.piestream.datasource.DataSource;
import org.piestream.datasource.FileDataSource;
import org.piestream.engine.Engine;
import org.piestream.engine.WindowType;
import org.piestream.events.Attribute;
import org.piestream.merger.BinTree;
import org.piestream.parser.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class Correct {

    private static final Logger logger = LoggerFactory.getLogger(Correct.class);

    /**
     * Builds a simple join query based on the given column count and window size.
     *
     * @param Col     Number of columns
     * @param windSize Window size for the query
     * @return The constructed query as a string
     */
    public static String buildSimpleJoinQuery(int Col, long windSize) {
        StringBuilder defineBuilder = new StringBuilder();
        StringBuilder patternBuilder = new StringBuilder();

        // Build the DEFINE part of the query
        for (int i = 1; i <= Col; i++) {
            if (i > 1) defineBuilder.append(", ");
            defineBuilder.append("A").append(i).append(" AS a_").append(i).append(" = 1 ");
        }

//        // Build the PATTERN part of the query
//        for (int i = 1; i < Col; i++) {
//            if (i > 1) patternBuilder.append(" AND ");
//            patternBuilder.append("A").append(i)
////                    .append(" meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals ")
//                    .append(" before  ")
//                    .append("A").append(i + 1);
//        }

//        String easyPattern=" A1 starts A2 AND A2 before A3 AND A3 overlaps A4 " ;
        String easyPattern = " A1 starts;overlaps  A2 AND A2 overlaps;starts  A3 AND A3  overlaps;starts A4 " ;
//        String easyPattern="   A3 before A2   " ;

        // Combine all parts into a complete query string
        String query = " FROM dataStream" +
                "\n DEFINE " + defineBuilder.toString() +
                "\n PATTERN " + easyPattern +
                "\n WITHIN " + windSize + " s " +
                "\n RETURN A1.ts, A1.te";

        return query;
    }

    /**
     * Builds the schema for the dataset based on the given column count.
     *
     * @param Col Number of columns
     * @return The schema for the dataset
     */
    public static Schema buildSchema(int Col) {
        List<Attribute> attriList = new ArrayList<>();
        attriList.add(new Attribute("id", "long"));
        for (int i = 1; i <= Col; i++) {
            attriList.add(new Attribute("a_" + i, "int"));
        }
        attriList.add(new Attribute("t1", "long"));
        attriList.add(new Attribute("t2", "long"));

        Schema schema = new Schema("CSV", "t1", attriList);

        return schema;
    }

    /**
     * Builds the runner that initializes the engine, processes the data, and returns the total processing time.
     *
     * @param col       Number of columns
     * @param limit     The maximum number of events to process
     * @param windSize  The window size for processing
     * @param basePath  The base path for data files
     * @param windowType The window type for processing (TIME_WINDOW or other types)
     * @return The total processing time in milliseconds
     */
    public static long buildRunner(int col, long limit, long windSize, String basePath, WindowType windowType) {
        Schema schema = buildSchema(col);
        String query = buildSimpleJoinQuery(col, windSize); // Assuming buildQuery is used here

        Engine engine = new Engine(schema, query, windowType);
        StringBuilder dataPath = new StringBuilder();
//        dataPath.append(basePath).append("events_col").append(col).append("_row").append(10000000).append(".csv");

        dataPath.append(basePath).append("bef_aft_col_4_id.csv");
        // Initialize FileDataSource and process data with the Engine
        try (DataSource dataSource = new FileDataSource(dataPath.toString(), limit)) {
            String line;
            long cnt = 0;
            long startTime = System.currentTimeMillis(); // Start timing
            while ((line = dataSource.readNext()) != null) {
                cnt++;
                engine.apply("", line); // Process each line of data
                if (cnt % (limit / 10) == 0) {
                    logger.info("Processed events num: " + cnt);
                    engine.printResultCNT();
                }

            }
            long endTime = System.currentTimeMillis();
            logger.info("\nTotal Lines Processed: " + (limit));
            logger.info("Processing time: " + (endTime - startTime) + " ms");
            engine.printResultCNT();
            engine.printAccumulatedTimes();
            engine.printAVGprocessTime();
            return (endTime - startTime);

        } catch (IOException e) {
            logger.error("Failed to open file: " + e.getMessage());
        }
        return 0;
    }

    /**
     * Main method to run the evaluation test. If command-line arguments are provided, use them; otherwise, run a default test.
     *
     * @param args Command-line arguments
     * @throws Exception If any error occurs during execution
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            int col = 4;
            long limit = 500000L;
            long windSize = 500000L;
            String dataPath = "/Users/czq/Code/TPS_data/";
            execute(col, limit, windSize, dataPath);
        } else {
            execute(Integer.valueOf(args[0]), Integer.valueOf(args[1]), Long.valueOf(args[2]), args[3]);
        }
    }

    /**
     * Executes the evaluation test with the given parameters.
     *
     * @param col      Number of columns
     * @param limit    The maximum number of events to process
     * @param windSize The window size for processing
     * @param basePath The base path for data files
     * @throws Exception If any error occurs during execution
     */
    private static void execute(int col, long limit, long windSize, String basePath) throws Exception {
        WindowType windowType = WindowType.TIME_WINDOW;
        logger.info("=====>  COL " + col + ", LIMIT " + limit + ", WINDSIZE " + windSize + ", DATAPATH " + basePath + " <=====");

        Long processedTime = buildRunner(col, limit, windSize, basePath, windowType);
    }
}
