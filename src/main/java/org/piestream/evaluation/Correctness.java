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

public class Correctness {

    private static final Logger logger = LoggerFactory.getLogger(Correctness.class);

    /**
     * Builds a simple join query based on the given column count and window size.
     *
     * @param Col     Number of columns
     * @param windSize Window size for the query
     * @return The constructed query as a string
     */
    public static String buildSimpleJoinQuery(int Col, long windSize,int existFinRel) {
        StringBuilder defineBuilder = new StringBuilder();
        StringBuilder patternBuilder = new StringBuilder();

        // Build the DEFINE part of the query
        for (int i = 1; i <= Col; i++) {
            if (i > 1) defineBuilder.append(", ");
            defineBuilder.append("A").append(i).append(" AS a_").append(i).append(" = 1 ");
        }
        String pattern;
        if(existFinRel==0){
            // Not include finishes/finished-by 不包含
            pattern = " A1  meets;met-by;overlaps;overlapped-by;finishes;finished-by   A2 " +
                              "AND A2  starts;started-by;contains;during;finishes;finished-by  A3 " +
                              "AND A3  before;after;equals;finishes;finished-by  A4 " ;
        }else{
            // Include finishes/finished-by
            pattern = " A1  meets;met-by;overlaps;overlapped-by   A2 " +
                    "AND A2  starts;started-by;contains;during  A3 " +
                    "AND A3  before;after;equals  A4 " ;
        }

        // Combine all parts into a complete query string
        String query = " FROM dataStream" +
                "\n DEFINE " + defineBuilder.toString() +
                "\n PATTERN " + pattern +
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
     * @param dataPath  The base path for data files
     * @param windowType The window type for processing (TIME_WINDOW or other types)
     * @return The total processing time in milliseconds
     */
    public static long buildRunner(int col, long limit, long windSize, String dataPath, WindowType windowType,int existFinRel) {
        Schema schema = buildSchema(col);
        String query = buildSimpleJoinQuery(col, windSize,existFinRel); // Assuming buildQuery is used here

        Engine engine = new Engine(schema, query, windowType);
        // Initialize FileDataSource and process data with the Engine
        try (DataSource dataSource = new FileDataSource(dataPath, limit)) {
            String line;
            long startTime = System.currentTimeMillis(); // Start timing
            long cnt=1;
            while ((line = dataSource.readNext()) != null) {
                engine.apply("", line); // Process each line of data
//                engine.showPercentage(cnt++,limit);
            }
            long endTime = System.currentTimeMillis();
            long processedTime=endTime - startTime;
//            logger.info("Total Lines Processed: " + (limit));
//            logger.info("Processing time: " + processedTime + " ms");
//            logger.info("Processing time: " + processedTime + " ms");
//            logger.info("RESULT: " + engine.getResultCNT());

//          CSV head:  method,PIEs,MPPs,events,wind_size,result,processed_time(ms),query_include_finish_rels
            StringBuilder resMsg=new StringBuilder();
            resMsg.append("PieStream,").append(col).append(",").append(col-1).append(",").append(limit).append(",")
                    .append(windSize).append(",").append(engine.getResultCNT()).append(",").append(processedTime)
                    .append(",").append(existFinRel);
            logger.info(resMsg.toString());
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
            long limit = 10000L;
            long windSize = limit;
//            String dataPath = "/Users/czq/Code/TPS_data/events_col4_row10000000.csv";
            String dataPath = "/home/uzi/Code/TPSdata/events_col4_row10000000.csv";
            int existFinRel=1;
            logger.info("method,PIEs,MPPs,events,wind_size,result,processed_time(ms),query_include_finish_rels");
            execute(col, limit, windSize, dataPath,existFinRel);
        } else {
            execute(Integer.valueOf(args[0]), Long.valueOf(args[1]),Long.valueOf(args[1]), args[2],Integer.valueOf(args[3]));
        }
    }

    /**
     * Executes the evaluation test with the given parameters.
     *
     * @param col           Number of columns
     * @param limit         The maximum number of events to process
     * @param windSize      The window size for processing
     * @param dataPath      The base path for data files
     * @param existFinRel   If the query includes rel "finishes" or "finished-by"
     * @throws Exception    If any error occurs during execution
     */
    private static void execute(int col, long limit, long windSize, String dataPath,int existFinRel) throws Exception {
        WindowType windowType = WindowType.TIME_WINDOW;
        Long processedTime = buildRunner(col, limit, windSize, dataPath, windowType,existFinRel);
    }
}
