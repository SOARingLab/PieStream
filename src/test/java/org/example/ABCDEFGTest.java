
package org.example;

import org.example.datasource.DataSource;
import org.example.datasource.FileDataSource;
import org.example.engine.Engine;
import org.example.parser.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ABCDEFGTest {

    @Test
    public void testEngineProcessing() {


        // Schema file path
        String schemaFilePath ="src/test/resources/domain/abcdefg.yaml";
        Schema schema = new Schema(schemaFilePath); // Load Schema

//        // Query statement
//        String query = "SELECT A.ts, B.te " +
//                "FROM CarStream " +
//                "DEFINE A AS a == 1 , B AS  b == 1 , C AS  c == 1 , D AS  d == 1 " +
//                "PATTERN A   meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals;before;after B " +
//                "AND C   meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals;before;after B " +
//                "AND D   meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals;before;after C " +
//                "WINDOW 10000";

        // Query statement
        String query = "SELECT A.ts, B.te " +
                "FROM CarStream " +
                "DEFINE A AS a > 0 , B AS  b  > 0 , C AS  c > 0 , D AS  d > 0 ,  E AS  e > 0  ,  F AS  f > 0 ,  G AS  g > 0 " +
                "PATTERN A    meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals  B " +
                "AND B   meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals    C " +
                "AND C   meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals   D " +
                "AND D meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals  E " +
                "AND E meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals  F " +
                "AND F meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals  G " +
                "WINDOW 10000";
        // Create Engine instance
        Engine engine = new Engine(schema, query);

        String ab13FilePath = "src/test/resources/data/eg_abcdefg_8.csv";
        // File data source, read data and apply to Engine
        try (DataSource dataSource = new FileDataSource(ab13FilePath)) {
            String line;
            long cnt=1;
            long lastResCNT=0;
            long resCNT=0;
            while ((line = dataSource.readNext()) != null) {
//                System.out.println("Line Read: " + line);
                engine.apply("", line); // Process each line of data
                resCNT = engine.getResultCNT();
                if(resCNT != lastResCNT){

                    System.out.println( " +  "+ (resCNT - lastResCNT)+ " |  "+ "Time : "+( cnt )+" Results : " + resCNT  );
                    lastResCNT =  resCNT;
                    engine.formatResult();
                }
                cnt++;

            }
//            engine.formatResult();
            engine.printResultCNT();
        } catch (IOException e) {
            System.err.println("Failed to open file: " + e.getMessage());
        }

        // TODO: Add assertions to verify the expected behavior of the engine
    }
}
