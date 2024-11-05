package org.example;


import org.example.datasource.DataSource;
import org.example.datasource.FileDataSource;
import org.example.engine.Engine;
import org.example.events.Attribute;
import org.example.parser.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BefAftTest {

    @Test
    public void testEngineProcessing() {

        // Schema file path
//        String schemaFilePath ="src/test/resources/domain/ab.yaml";
        List<Attribute> attriList= new ArrayList<Attribute>();
        attriList.add(new Attribute("a","int"));
        attriList.add(new Attribute("b","int"));
        Schema schema = new Schema("CSV", attriList); // Load Schema

        // Query statement
        String query = "SELECT A.ts, B.te " +
                "FROM CarStream " +
                "DEFINE A AS a == 1 , B AS  b == 1  " +
                "PATTERN A  before;after  B " +
                "WINDOW 10000";
        // Create Engine instance
        Engine engine = new Engine(schema, query);

        String ab13FilePath = "src/test/resources/data/beforeAfter.csv";
        // File data source, read data and apply to Engine
        try (DataSource dataSource = new FileDataSource(ab13FilePath)) {
            String line;
            while ((line = dataSource.readNext()) != null) {
//                System.out.println("Line Read: " + line);
                engine.apply("", line); // Process each line of data
            }
            engine.printResultCNT();
            engine.formatResult();
        } catch (IOException e) {
            System.err.println("Failed to open file: " + e.getMessage());
        }

        // TODO: Add assertions to verify the expected behavior of the engine
    }
}
