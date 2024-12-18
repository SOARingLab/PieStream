package org.piestream.merger;

import org.piestream.engine.Window;
import org.piestream.engine.WindowType;
import org.piestream.piepair.IEP;
import org.piestream.piepair.eba.EBA;

import java.util.*;

public class Table {
    private final LinkList<Row> rows;
    private long size; // Current number of rows
    private final Window window;
    private final Map<String, List<Row>> hashIndex;

    // Performance tracking variables
    public static long removeRowsAndIndexTime;
    public static long concateTime;
    public static long concateRebuilTime;
    public static long addRowsTime;
    public static long clearRowsTime;
    public static long addRowMergeTime;
    public static long startTime;
    public static long endTime;


    public Table(Window window ) {
        this.window = window;
        this.size = 0;
        this.hashIndex = new HashMap<>();
        this.rows = new LinkList<Row>(window);
    }

    /**
     * Updates the row's end time based on the provided predicate and start/end times.
     *
     * @param pred       The predicate used for updating the row
     * @param startTime  The start time for the predicate
     * @param endTime    The end time for the predicate
     */
    public void update(String pred, Long startTime, Long endTime){
        LinkList<Row>.Node node = rows.getHead();
        while (node != null) {
            Row row = node.getData();
            String searchColName = pred + ".ST";
            if (row.getValueFromColName(searchColName) == startTime) {
                row.update(pred + ".ET", endTime);
            } else if (row.getValueFromColName(searchColName) < startTime) {
                node = node.next;
            } else {
                return;
            }
        }
    }

    /**
     * Returns the hash index of the table.
     *
     * @return A map representing the hash index of rows
     */
    public Map<String, List<Row>> getHashIndex(){
        return hashIndex;
    }

    /**
     * Deletes rows and updates the index by removing the oldest rows.
     *
     * @param toDelSize The number of rows to delete
     */
    private void deleteRowsAndIndex(long toDelSize) {
        if (toDelSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Excess size exceeds Integer.MAX_VALUE, cannot proceed.");
        }
        if (toDelSize > window.getWindowCapacity()) {
            toDelSize = window.getWindowCapacity();
        }
        int cnt = 0;
        while (cnt < toDelSize) {
            Row oldestRow = this.rows.deleteHead();
            deleteHashindexByRow(oldestRow);
            cnt++;
        }
        size -= toDelSize;
    }

    /**
     * Removes the given row from the hash index.
     *
     * @param row The row to remove from the hash index
     */
    private void deleteHashindexByRow(Row row) {
        String indexKey = row.getIndexKey();
        List<Row> sameIndexRowList = hashIndex.get(indexKey);
        if (sameIndexRowList != null) {
            sameIndexRowList.remove(row);
            if (sameIndexRowList.isEmpty()) {
                hashIndex.remove(indexKey);
            }
        }
    }

    /**
     * Adds multiple rows to the table without generating an index.
     *
     * @param rowsToAdd The rows to add to the table
     */
    public void addRowsWithoutGenIndex(LinkList<Row> rowsToAdd) {
        long excess = size + rowsToAdd.getSize() - this.getCapacity();

        if (window.getWindowType() == WindowType.CAPACITY_WINDOW && excess > 0) {
            startTime = System.currentTimeMillis();
            deleteRowsAndIndex(excess);
            endTime = System.currentTimeMillis();
            removeRowsAndIndexTime += (endTime - startTime);
        }

        startTime = System.currentTimeMillis();
        // Batch add new rows
        rows.concat(rowsToAdd);
        size += rowsToAdd.getSize();
        endTime = System.currentTimeMillis();
        addRowsTime += (endTime - startTime);
    }

    /**
     * Adds a single row to the table.
     *
     * @param row The row to add
     */
    public void addRow(Row row) {
        // Check if the capacity is full
        if (window.getWindowType() == WindowType.CAPACITY_WINDOW && size >= this.getCapacity()) {
            deleteRowsAndIndex(1);
        }

        // Add new row
        rows.safeAdd(row);
        size++;

        // Update hash index
        String newIndexKey = row.getIndexKey();
        if (newIndexKey == "") {
            throw new IllegalArgumentException("String parameter cannot be null");
        }
        hashIndex.computeIfAbsent(newIndexKey, k -> new ArrayList<>()).add(row);
    }

    /**
     * Adds a row using an IEP and EBA mapping.
     *
     * @param iep            The IEP instance to create the row
     * @param EBA2String     A map of EBA to string values
     * @param joinColumns    A list of columns used for joining
     */
    public void addRow(IEP iep, Map<EBA, String> EBA2String, List<String> joinColumns) {
        addRow(new Row(iep, EBA2String, joinColumns));
    }

    /**
     * Returns all rows in the table.
     *
     * @return A list of all rows in the table
     */
    public LinkList<Row> getRows() {
        return rows;
    }

    /**
     * Returns the current number of rows in the table.
     *
     * @return The current number of rows
     */
    public long getSize() {
        return size;
    }

    /**
     * Returns the capacity of the table.
     *
     * @return The table's capacity
     */
    public long getCapacity() {
        return window.getWindowCapacity();
    }

    /**
     * Returns the window associated with the table.
     *
     * @return The window instance
     */
    public Window getWindow() {
        return window;
    }

    /**
     * Concatenates another table's rows into the current table.
     *
     * @param otherTable The other table to concatenate
     */
    public void concatenate(Table otherTable) {
        if (otherTable == null) {
            return;
        }
        long concatST = System.currentTimeMillis();
        if (otherTable.getSize() != 0) {
            LinkList<Row> otherRows = otherTable.getRows();

            // Add all rows from otherTable
            long addRowMergeST = System.currentTimeMillis();
            addRowsWithoutGenIndex(otherRows);
            MapMerger.mergeSimpleMaps(hashIndex, otherTable.getHashIndex());
            long addRowMergeET = System.currentTimeMillis();
            addRowMergeTime += (addRowMergeET - addRowMergeST);
        }
        long concatET = System.currentTimeMillis();
        concateTime += (concatET - concatST);
    }

    /**
     * Concatenates another table's rows into the current table with custom join columns.
     *
     * @param otherTable           The other table to concatenate
     * @param tableJoinCols        The join columns from the current table
     * @param addedTableJoinCols   The join columns from the added table
     */
    public void concatenate(Table otherTable, List<String> tableJoinCols, List<String> addedTableJoinCols) {
        if (otherTable == null) {
            return;
        }
        if (tableJoinCols == addedTableJoinCols) {
            concatenate(otherTable);
        } else {
            concatenateRebuildIndex(otherTable, tableJoinCols);
        }
    }

    /**
     * Concatenates another table's rows into the current table, rebuilding indexes if necessary.
     *
     * @param otherTable       The other table to concatenate
     * @param newJoinColumns   The new join columns for index rebuilding
     */
    public void concatenateRebuildIndex(Table otherTable, List<String> newJoinColumns) {
        if (otherTable == null) {
            return;
        }
        long CRstartTime = System.currentTimeMillis();
        if (otherTable.getSize() != 0) {
            LinkList<Row> otherRows = otherTable.getRows();

            LinkList<Row>.Node node = otherRows.getHead();
            while (node != null) {
                Row row = node.getData();
                addRow(new Row(row.getTimeData(), newJoinColumns, row.getSource(), row.getTriggerTime(), true));
                node = node.next;
            }
        }

        long CRendTime = System.currentTimeMillis();
        concateRebuilTime += (CRendTime - CRstartTime);
    }

    /**
     * Adds a value to the specified column and returns the accumulated process time.
     *
     * @param colName   The column name
     * @param value     The value to add to the column
     * @return          The accumulated process time
     */
    public long addDetectTimeAndCalProcessTime(String colName, long value) {
        LinkList<Row>.Node nd = rows.getHead();
        long accumlatedProcessTime = 0;
        while (nd != null) {
            nd.getData().addCol(colName, value);
            accumlatedProcessTime += nd.getData().getProcessTime(value);
            nd = nd.next;
        }
        return accumlatedProcessTime;
    }

    /**
     * Clears all rows from the table and resets the size.
     */
    public void clear() {
        rows.clear(); // Clear row list
        size = 0; // Reset the number of rows
        hashIndex.clear(); // Clear the hash index
    }

    /**
     * Refreshes the index by removing rows from the hash index that have been deleted.
     *
     * @param deadLine   The deadline for row removal
     * @param toDelRows  The rows to be deleted
     */
    public void refreshIndex(long deadLine, List<Row> toDelRows) {
        for (Row row : toDelRows) {
            deleteHashindexByRow(row);
        }
    }

    /**
     * Refreshes the table by removing rows that have passed the deadline.
     *
     * @param deadLine The deadline for row removal
     */
    public void refresh(long deadLine) {
        List<Row> toDelRows = rows.refresh(deadLine);
        size -= (toDelRows.size());
        refreshIndex(deadLine, toDelRows);
    }
}
