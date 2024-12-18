package org.piestream.merger;

import java.util.*;

/**
 * The HashJoiner class performs hash-based join operations between two tables.
 * The join is done based on multiple join columns.
 */
public class HashJoiner {
    // The total time spent on join operations across all invocations
    public static long searchForJoin = 0;
    // The start time of the join operation
    public static long startTime = 0;
    // The end time of the join operation
    public static long endTime = 0;

    // The count of the number of join operations performed
    public static long joinCNT = 0;

    /**
     * Performs a hash join between two tables based on the specified join columns.
     * The smaller table is used for creating a hash index, and the larger table is used for probing.
     *
     * @param table1 The first table to join
     * @param table2 The second table to join
     * @param parentJoinColumns The columns on which to perform the join
     * @param needNewIndex Indicates whether a new index is needed for the join
     * @return A table resulting from the hash join
     */
    public static Table hashJoin(Table table1, Table table2, List<String> parentJoinColumns, boolean needNewIndex) {

        // If either table is empty, return null as no join can be performed
        if (table1.getSize() * table2.getSize() == 0) {
            return null;
        }

        // Determine the smaller and larger tables based on their sizes
        Table smallerTable = table1.getRows().getSize() <= table2.getRows().getSize() ? table1 : table2;
        Table largerTable = table1.getRows().getSize() > table2.getRows().getSize() ? table1 : table2;

        // Capture the start time of the join operation
        startTime = System.currentTimeMillis();

        // Perform a quick join using hash indices
        // Table resultTable = JoinTwoTableNaive(smallerTable, largerTable, parentJoinColumns, needNewIndex);
        Table resultTable = JoinTwoTableQuick(smallerTable, largerTable, parentJoinColumns, needNewIndex);

        // Capture the end time of the join operation
        endTime = System.currentTimeMillis();
        // Update the total time spent on join operations
        searchForJoin += (endTime - startTime);
        return resultTable;
    }

    /**
     * A quick hash join implementation that uses hash indices to join rows from the two tables.
     * It compares hash indices of both tables and performs the join for matching index keys.
     *
     * @param smallerTable The smaller table (used for creating the hash index)
     * @param largerTable The larger table (used for probing the hash index)
     * @param parentJoinColumns The columns on which to perform the join
     * @param needNewIndex Indicates whether a new index is needed for the join
     * @return A table resulting from the hash join
     */
    public static Table JoinTwoTableQuick(Table smallerTable, Table largerTable, List<String> parentJoinColumns, boolean needNewIndex) {
        // Create a result table to hold the joined rows
        Table resultTable = new Table(largerTable.getWindow());

        // Get the hash indices for both tables
        Map<String, List<Row>> smallHashIndex = smallerTable.getHashIndex();
        Map<String, List<Row>> largeHashIndex = largerTable.getHashIndex();

        // Find the shared index keys between the two tables
        Set<String> sharedIndex = new HashSet<>(smallHashIndex.keySet());
        sharedIndex.retainAll(largeHashIndex.keySet());

        // Perform the join for each shared index key
        for (String indexKey : sharedIndex) {
            // Iterate over the rows in the larger table with the matching index key
            for (Row largeRow : largeHashIndex.get(indexKey)) {
                // Iterate over the rows in the smaller table with the matching index key
                for (Row smallRow : smallHashIndex.get(indexKey)) {
                    // Add the joined row to the result table
                    resultTable.addRow(largeRow.join(smallRow, parentJoinColumns, needNewIndex));
                    // Increment the join count
                    joinCNT++;
                }
            }
        }
        return resultTable;
    }
}
