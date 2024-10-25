
package org.example;

import org.example.datasource.DataSource;
import org.example.datasource.FileDataSource;
import org.example.engine.Engine;
import org.example.parser.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class AZTest {

    @Test
    public void testEngineProcessing() {


        // Schema file path
        String schemaFilePath ="src/test/resources/domain/az.yaml";
        Schema schema = new Schema(schemaFilePath); // Load Schema

        // Query statement
        String query = "SELECT A.ts, B.te " +
                "FROM CarStream " +
                "DEFINE A AS a > 0, B AS b > 0, C AS c > 0, D AS d > 0, E AS e > 0, F AS f > 0, G AS g > 0, " +
                "H AS h > 0, I AS i > 0, J AS j > 0, K AS k > 0, L AS l > 0, M AS m > 0, " +
                "N AS n > 0, O AS o > 0, P AS p > 0, Q AS q > 0, R AS r > 0, " +
                "S AS s > 0, T AS t > 0, U AS u > 0, V AS v > 0, W AS w > 0, " +
                "X AS x > 0, Y AS y > 0, Z AS z > 0 " +
                "PATTERN A meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals B " +
                "AND B meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals C " +
                "AND C meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals D " +
                "AND D meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals E " +
                "AND E meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals F " +
                "AND F meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals G " +
                "AND G meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals H " +
                "AND H meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals I " +
                "AND I meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals J " +
                "AND J meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals K " +
                "AND K meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals L " +
                "AND L meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals M " +
                "AND M meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals N " +
                "AND N meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals O " +
                "AND O meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals P " +
                "AND P meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals Q " +
                "AND Q meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals R " +
                "AND R meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals S " +
                "AND S meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals T " +
                "AND T meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals U " +
                "AND U meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals V " +
                "AND V meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals W " +
                "AND W meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals X " +
                "AND X meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals Y " +
                "AND Y meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals Z " +
                "WINDOW 10000";

        // Create Engine instance
        Engine engine = new Engine(schema, query);

        String ab13FilePath = "src/test/resources/data/col26_row5000.csv";
        // File data source, read data and apply to Engine
        try (DataSource dataSource = new FileDataSource(ab13FilePath)) {
            String line;
            long cnt=1;
            long lastResCNT=0;
            long resCNT=0;
            long startTime = System.currentTimeMillis(); // 开始计时
            while ((line = dataSource.readNext()) != null) {
//                System.out.println("Line Read: " + line);
                engine.apply("", line); // Process each line of data
//                resCNT = engine.getResultCNT();
                if(resCNT != lastResCNT){

//                    System.out.println( " +  "+ (resCNT - lastResCNT)+ " |  "+ "Time : "+( cnt )+" Results : " + resCNT  );
//                    lastResCNT =  resCNT;
//                    System.out.println(cnt);
//                    engine.formatResult();
                }
                cnt++;
                if(cnt%100==0){
                    System.out.println(cnt);
                }
            }
            engine.formatResult();
            System.out.println( cnt);

            engine.printResultCNT();

            long endTime = System.currentTimeMillis(); // 结束计时
            long duration = endTime - startTime; // 计算持续时间
            System.out.println("Processing time: " + duration + " ms");

            engine.printAccumulatedTimes();

        } catch (IOException e) {
            System.err.println("Failed to open file: " + e.getMessage());
        }

        // TODO: Add assertions to verify the expected behavior of the engine
    }
}
