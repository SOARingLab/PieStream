package org.piestream.evaluation;

import org.piestream.datasource.DataSource;
import org.piestream.datasource.FileDataSource;
import org.piestream.engine.Engine;
import org.piestream.engine.RuntimeSet;
import org.piestream.engine.WindowType;
import org.piestream.events.Attribute;
import org.piestream.parser.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class LNGRollover {

    private static final Logger logger = LoggerFactory.getLogger(LNGRollover.class);

    /**
     * Builds a simple join query based on the given column count and window size.
     *
     * @param windSize Window size for the query
     * @return The constructed query as a string
     */
    public static String buildSimpleJoinQuery(  long windSize , int usePreciseRel) {


//        DEFINE
//        A As SOBOR  > 0 && BOR < 5*NBOR [3h,10d]
//        B As BOR > NBOR*10 [10m,50m]
//        C As SOBOR  < 0 && BOR <5*NBOR && BOR >0 [20m,70m]
//        D As DD < 1 [30m,]
//        PATTERN
//        A before B
//        AND B before C
//        AND B during; starts; overlaps D
//        AND D overlaps; finished-by; contains C
//        AND A before;meets;overlaps D

        //NBOR =10
        String define ="A AS ((SOBOR  > 0) & (BOR < 50)) , " +
                " B AS BOR > 100 , "+
                " C AS ((SOBOR  < 0) & (BOR < 50) & (BOR >0)) , " +
                " D AS DD < 1  ";
        String pattern;
        if (usePreciseRel==1){
            pattern ="        A followed-by  B "+
                            "        AND B followed-by C " +
                            "        AND B during; starts; overlaps D " +
                            "        AND D overlaps; finished-by; contains C " +
                            "        AND A followed-by;meets;overlaps D ";

        }else{
            pattern ="        A before  B "+
                    "        AND B before C " +
                    "        AND B during; starts; overlaps D " +
                    "        AND D overlaps; finished-by; contains C " +
                    "        AND A before;meets;overlaps D ";
        }

        // Combine all parts into a complete query string
        String query = " FROM LNG_Stream" +
                "\n DEFINE " + define +
                "\n PATTERN " + pattern +
                "\n WITHIN " + windSize + " s " +
                "\n RETURN A1.ts, A1.te";

        return query;
    }

    /**
     * Builds the schema for the dataset based on the given column count.
     *
     * @return The schema for the dataset
     */
    public static Schema buildSchema( ) {
//        times_cnt  time_stamp   BOR  SOBOR   DD
        List<Attribute> attriList = new ArrayList<>();
        attriList.add(new Attribute("id", "long"));
        attriList.add(new Attribute("times_cnt", "long"));
        attriList.add(new Attribute("time_stamp", "long"));
        attriList.add(new Attribute("BOR", "float"));
        attriList.add(new Attribute("SOBOR", "float"));
        attriList.add(new Attribute("DD", "float"));
        Schema schema = new Schema("CSV", "time_stamp", attriList);
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
    public static long buildRunner( long limit, long windSize, String dataPath, WindowType windowType,int usePreciseRel  ) {
        Schema schema = buildSchema( );
        String query = buildSimpleJoinQuery( windSize,usePreciseRel ); // Assuming buildQuery is used here
        RuntimeSet.initialize(false, false); // 根据需要设置配置项
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

//          CSV head:  method,PIEs,MPPs,events,wind_size,result,processed_time(ms)
            StringBuilder resMsg=new StringBuilder();
            resMsg.append("PieStream,").append(4).append(",").append(5).append(",").append(limit).append(",")
                    .append(windSize).append(",").append(engine.getResultCNT()).append(",").append(processedTime)
                    .append(",").append(usePreciseRel);
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
        if (args.length < 1) {
            long limit = 4320000L;
            long windSize = 4320000L;
            URL resource  =  Correctness.class.getClassLoader().getResource("data/LNGRollover_8633_45.csv");
            String dataPath = Paths.get(resource.toURI()).toAbsolutePath().toString();
            int usePreciseRel = 0;
            logger.info("method,PIEs,MPPs,events,wind_size,result,processed_time(ms),usePreciseRel");
            buildRunner( limit, windSize, dataPath, WindowType.TIME_WINDOW,usePreciseRel );

        } else {
            long limit = Long.valueOf(args[0] );
            long windSize =  Long.valueOf(args[1] );
            String dataPath= args[2];
            int usePreciseRel = Integer.valueOf(args[3]);
            buildRunner( limit, windSize, dataPath, WindowType.TIME_WINDOW,usePreciseRel );
        }
    }
}
