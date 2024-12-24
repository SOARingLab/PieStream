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

        String patternA4;
        // Build the PATTERN part of the query
//        for (int i = 1; i < Col; i++) {
//            if (i > 1) patternBuilder.append(" AND ");
//            patternBuilder.append("A").append(i)
////                    .append(" meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals ")
////                    .append(" meets;met-by;overlapped-by;starts;started-by;during;contains ")
//                    .append(" meets;overlaps;overlapped-by;starts;started-by;contains ")
//                    .append("A").append(i + 1);
//        }
         patternA4=" A1 starts A2 AND A2 before A3 AND A3 overlaps A4";
//        patternA4 = patternBuilder.toString();

        // Combine all parts into a complete query string
        String query =
                " FROM dataStream " +
                        "\n DEFINE " + defineBuilder.toString() +
                        "\n PATTERN " + patternA4 +
                        "\n WITHIN " + windSize + " S " +
                        "\n RETURN A1.ts, A1.te ";

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
     * Initializes the engine, processes data, and returns the total processing time.
     *
     * @param col       Number of columns
     * @param limit     The maximum number of events to process
     * @param windSize  The window size for processing
     * @param dataPath  The base path for data files
     * @param rate      The rate at which data is processed
     * @param windowType The window type for processing (TIME_WINDOW or other types)
     * @return The total processing time in milliseconds
     */
    public static long buildRunner(int col, long limit, long windSize, String dataPath, long rate, WindowType windowType) {
        Schema schema = buildSchema(col);
        String query = buildSimpleJoinQuery(col, windSize); // Assuming buildQuery is used here

        Engine engine = new Engine(schema, query, windowType);

        // Initialize FileDataSource and process data with the Engine
        try (DataSource dataSource = new FileDataSource(dataPath.toString(), limit, rate)) {
            String line;
            long startTime = System.currentTimeMillis(); // Start timing
            while ((line = dataSource.readNext()) != null) {
                engine.apply("", line); // Process each line of data
            }
            long endTime = System.currentTimeMillis();
            long processedTime=endTime - startTime;
            long avgLatency=engine.getAVGprocessLatency();
//            logger.info("Total Lines Processed: " + (limit));
//            logger.info("Processing time: " + processedTime + " ms");
//            logger.info("Processing time: " + processedTime + " ms");
//            logger.info("RESULT: " + engine.getResultCNT());

//            method,PIEs,MPPs,events,wind_size,rates,avg_process_latency,result,processed_time(ms)
            StringBuilder resMsg=new StringBuilder();
            resMsg.append("PieStream,").append(col).append(",").append(col-1).append(",").append(limit).append(",")
                    .append(windSize).append(",").append(rate).append(",").append(avgLatency).append(",").append(engine.getResultCNT())
                    .append(",").append(processedTime);
            logger.info(resMsg.toString());

            return (endTime - startTime);

        } catch (IOException e) {
            System.err.println("Failed to open file: " + e.getMessage());
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
            logger.info("Running Test : ");
            int col = 4;
            long limit = 100000L;
            long windSize = 100000L;
            String dataPath = "/Users/czq/Code/TPS_data/events_col4_row10000000.csv";
            long rate = 100000;
            logger.info("method,PIEs,MPPs,events,wind_size,rates,avg_process_latency(ns),result,processed_time(ms)");

            execute(col, limit, windSize, dataPath, rate);
        } else {
            execute(Integer.valueOf(args[0]), Integer.valueOf(args[1]), Long.valueOf(args[2]), args[3], Long.valueOf(args[4]));
        }
    }

    /**
     * Executes the evaluation test with the given parameters.
     *
     * @param col      Number of columns
     * @param limit    The maximum number of events to process
     * @param windSize The window size for processing
     * @param dataPath The base path for data files
     * @param rate     The rate at which data is processed
     * @throws Exception If any error occurs during execution
     */
    private static void execute(int col, long limit, long windSize, String dataPath, long rate) throws Exception {
        WindowType windowType = WindowType.TIME_WINDOW;
        Long processedTime = buildRunner(col, limit, windSize, dataPath, rate, windowType);
    }
}
