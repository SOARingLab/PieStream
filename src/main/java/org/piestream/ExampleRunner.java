package org.piestream;

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

/**
 * The ExampleRunner class is designed to execute a series of example runs for testing the performance of the stream processing engine.
 * It constructs dynamic queries for join operations, builds schemas, and runs simulations on streaming data, evaluating the processing time for various configurations.
 * The class supports the creation of queries with different column counts and row limits, and it processes input data from CSV files using a defined schema and windowing type.
 *
 */
public class ExampleRunner {

    private static final Logger logger = LoggerFactory.getLogger(ExampleRunner.class);

    /**
     * Builds a simple join query string for the given number of columns.
     *
     * @param Col The number of columns to include in the query.
     * @return The constructed SQL-like query string.
     */
    public static String buildSimpleJoinQuery(int Col ) {
        StringBuilder defineBuilder = new StringBuilder();
        StringBuilder patternBuilder = new StringBuilder();

        // Build the DEFINE part of the query
        for (int i = 1; i <= Col; i++) {
            if (i > 1) defineBuilder.append(", ");
            defineBuilder.append("A").append(i).append(" AS a_").append(i).append(" = 1 ");
        }

        // Build the PATTERN part of the query
        for (int i = 1; i < Col; i++) {
            if (i > 1) patternBuilder.append(" AND ");
            patternBuilder.append("A").append(i)
                    .append(" meets;overlaps;overlapped-by;starts;started-by;contains ")
                    .append("A").append(i + 1);
        }

        // Combine all parts into a complete query string
        String query = " FROM dataStream" +
                "\n DEFINE " + defineBuilder.toString() +
                "\n PATTERN " + patternBuilder.toString() +
                "\n WITHIN 100000 S"+
                "\n RETURN A1.ts, A1.te ";

        return query;
    }

    /**
     * Builds a schema for the data stream based on the number of columns.
     *
     * @param Col The number of columns in the schema.
     * @return A Schema object containing the attributes for the data stream.
     */
    public static Schema buildSchema(int Col) {
        List<Attribute> attriList = new ArrayList<>();
        for (int i = 1; i <= Col; i++) {
            attriList.add(new Attribute("a_" + i, "int"));
        }
        attriList.add(new Attribute("ts","long"));
        attriList.add(new Attribute("te","long"));
        return new Schema("CSV","ts",  attriList);
    }

    /**
     * Initializes the stream processing engine and processes the input data, measuring the processing time.
     *
     * @param col The number of columns in the schema.
     * @param limit The limit on the number of rows to process.
     * @param basePath The base path where the data file is located.
     * @param windowType The type of windowing used in the stream processing.
     * @return The processing time in milliseconds.
     */
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

            logger.info("\nTotal Lines Processed: " + (limit));
            logger.info("Processing time: " + (endTime - startTime) + " ms");
            engine.printResultCNT();
            engine.printAccumulatedTimes();
            return  (endTime - startTime);

        } catch (IOException e) {
            System.err.println("Failed to open file: " + e.getMessage());
        }
        return 0;
    }

    /**
     * The main entry point for running the example with different configurations of column counts and row limits.
     *
     * @param args Command-line arguments (not used).
     */
    public static void main(String[] args) {

        List<Integer> colList = new ArrayList<>(Arrays.asList(4 ));
        List<Long> limitList = new ArrayList<>(Arrays.asList( 1_000_000L ));
        WindowType windowType=WindowType.TIME_WINDOW;
//        String basePath = "/home/uzi/Code/TPSdata/";

        String basePath = "/Users/czq/Code/TPS_data/";
        Map<Integer, Map<Long, Long>> col_row_proceTimeMap = new HashMap<>();

        for (int col : colList) {
            // Use computeIfAbsent to ensure each col corresponds to a new Map<Long, Long>
            col_row_proceTimeMap.computeIfAbsent(col, k -> new HashMap<>());

            for (long limit : limitList) {
                logger.info("===============  COL "+col+", LIMIT "+limit+" ===============");
                Long processedTime = buildRunner(col, limit, basePath, windowType );
                col_row_proceTimeMap.get(col).put(limit, processedTime);
                logger.info("timeUsed: "+col_row_proceTimeMap);
            }
        }

    }
}
