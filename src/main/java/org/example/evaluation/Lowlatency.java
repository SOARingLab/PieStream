//package org.example.evaluation;
//
//import org.example.Main;
//import org.example.datasource.DataSource;
//import org.example.datasource.FileDataSource;
//import org.example.engine.Engine;
//import org.example.engine.WindowType;
//import org.example.parser.Schema;
//
//import java.io.IOException;
//
//public class Lowlatency {
//
//
//
//
//    public static void main(String[] args) {
//
//        WindowType windowType=WindowType.TIME_WINDOW;
//        Schema schema = Main.buildSchema(col);
//        String query =  "";
//        Engine engine = new Engine(schema, query, windowType);
//
//        String dataPath = "";
//        // Initialize FileDataSource and process data with the Engine
//        try (
//                DataSource dataSource = new FileDataSource(dataPath)) {
//            String line;
//            long cnt = 0;
//            long startTime = System.currentTimeMillis(); // Start timing
//            while ((line = dataSource.readNext()) != null && cnt<row) {
//                engine.apply("", line); // Process each line of data
//                cnt++;
////                if (cnt % 100000 == 0) {
////                    System.out.println(cnt + ", ");
////                }
//            }
//            long endTime = System.currentTimeMillis();
//
//            System.out.println("\nTotal Lines Processed: " + (cnt));
//            System.out.println("Processing time: " + (endTime - startTime) + " ms");
//            engine.printResultCNT();
//            engine.printAccumulatedTimes();
//        } catch (
//                IOException e) {
//            System.err.println("Failed to open file: " + e.getMessage());
//        }
//
//    }
//
//}
