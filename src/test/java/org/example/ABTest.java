
package org.example;

import org.example.datasource.DataSource;
import org.example.datasource.FileDataSource;
import org.example.engine.Engine;
import org.example.events.Attribute;
import org.example.parser.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ABTest {

    @Test
    public void testEngineProcessing() {


        // Schema file path
        String schemaFilePath ="src/test/resources/domain/ab.yaml";
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
                "DEFINE A AS a > 0 , B AS  b  > 0  " +
                "PATTERN A   before;after   B " +
//                "PATTERN A  meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals   B " +
                "WINDOW 10000";

//        meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals
        // Create Engine instance
        Engine engine = new Engine(schema, query);

        String ab13FilePath = "/Users/czq/Code/TPstream0/TPStream_DAPD/jepc-v2/col2_row10.csv";
        // File data source, read data and apply to Engine
        try (DataSource dataSource = new FileDataSource(ab13FilePath)) {
            String line;
            long cnt=1;
            while ((line = dataSource.readNext()) != null) {
//                System.out.println("Line Read: " + line);
//                System.out.println(cnt++);
                engine.apply("", line); // Process each line of data
//                engine.formatResult();
            }
//            engine.formatResult();
            engine.printResultCNT();
        } catch (IOException e) {
            System.err.println("Failed to open file: " + e.getMessage());
        }

        // TODO: Add assertions to verify the expected behavior of the engine
    }
}
