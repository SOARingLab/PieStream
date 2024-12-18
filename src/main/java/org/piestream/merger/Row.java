package org.piestream.merger;

import org.piestream.events.Expirable;
import org.piestream.piepair.IEP;
import org.piestream.piepair.eba.EBA;

import java.util.*;

public class Row implements Expirable {
    private Map<String, Long> timeData;  // A map storing column names and their corresponding time values
    private Set<Expirable> source;       // A set of Expirable objects that are the source of this row
    private long triggerTime;            // The earliest trigger time from the IEP, determining the expiration time of this row
    private String indexKey;             // The index key for this row


    // Constructor that accepts a map of column names and values, a list of join columns, a set of source Expirables, trigger time, and a flag to generate a new index
    public Row(Map<String, Long> timeData, List<String> joinColumns, Set<Expirable> source, long triggerTime, boolean needNewIndex) {
        this.triggerTime = triggerTime;
        this.timeData = timeData;
        this.source = source;
        if (needNewIndex) {  // Non-root nodes need an index
            this.indexKey = generateKey(joinColumns);
        } else {  // Root node does not need an index
            this.indexKey = null;
        }
    }

    // Constructor to create a Row from leaf nodes, which only contains IEP information
    public Row(IEP iep, Map<EBA, String> EBA2String, List<String> joinColumns) {

        // Generate column names and values based on the IEP and EBA mapping
        String formerPieSTKey = EBA2String.get(iep.getFormerPie()) + ".ST";  // formerPieST column name
        String latterPieSTKey = EBA2String.get(iep.getLatterPie()) + ".ST";  // latterPieST column name
        String triggerName = EBA2String.get(iep.getFormerPie()) + "-" + EBA2String.get(iep.getLatterPie()) + "_triggerTime";

        this.timeData = new HashMap<>();
        this.timeData.put(formerPieSTKey, iep.getFormerStartTime());  // Store the start time of the former pie
        this.timeData.put(latterPieSTKey, iep.getLatterStartTime());  // Store the start time of the latter pie
        this.timeData.put(triggerName, iep.getSystemTriggerTime());  // Store the trigger time
        this.source = new HashSet<>();
        this.source.add(iep);  // Add the IEP to the source set
        this.triggerTime = iep.getTriggerTime();  // Set the trigger time from the IEP
        this.indexKey = generateKey(joinColumns);  // Generate the index key for this row
    }

    public Set<Expirable> getSource() {
        return source;  // Return the source set of Expirable objects
    }

    public long getTriggerTime() {
        return triggerTime;  // Return the trigger time of this row
    }

    // Update the value of a column by replacing it with a new end time
    public void update(String replaceColName, Long EndTime) {
        this.timeData.put(replaceColName, EndTime);  // Replace the column's value with the new end time
    }

    public String getIndexKey() {
        return indexKey;  // Return the index key of this row
    }

    // Get the names of all columns in this row
    public Set<String> getColumnNames() {
        return timeData.keySet();  // Return the set of column names from the timeData map
    }

    // Generate a unique key for this row based on the specified join columns
    private String generateKey(List<String> joinColumns) {
        if (joinColumns == null) {  // Root nodes don't need an index key
            throw new IllegalArgumentException("String parameter cannot be null");
        }
        StringBuilder keyBuilder = new StringBuilder();
        for (String column : joinColumns) {
            if (column == null) {  // Ensure no column name is null
                throw new IllegalArgumentException("String parameter cannot be null");
            }
            keyBuilder.append(timeData.get(column + ".ST")).append("|");  // Append the start time of the column to the key
        }
        return keyBuilder.toString();  // Return the generated key
    }

    // Perform a natural join between this row and another row, producing a new row
    public Row join(Row other, List<String> parentJoinColumns, boolean needNewIndex) {
        long earlyTime = this.triggerTime <= other.triggerTime ? this.triggerTime : other.triggerTime;  // Use the earliest trigger time
        Map<String, Long> joinedData = new HashMap<>(this.timeData);  // Create a new map for the joined data
        joinedData.putAll(other.timeData);  // Add the other row's time data to the joined data
        Set<Expirable> mergedIEPSource = new HashSet<>(this.getSource());  // Merge the source sets of both rows
        mergedIEPSource.addAll(other.getSource());
        Row r = new Row(joinedData, parentJoinColumns, mergedIEPSource, earlyTime, needNewIndex);  // Create a new Row with the merged data
        return r;
    }

    public Map<String, Long> getTimeData() {
        return timeData;  // Return the map of time data for this row
    }

    // Get the value of a specific column by its name
    public Long getValueFromColName(String colName) {
        return timeData.get(colName);  // Return the value for the specified column name
    }

    // Return a string representation of this row (for debugging or logging purposes)
    @Override
    public String toString() {
        return timeData.toString();  // Return the time data as a string
    }

    @Override
    public boolean isExpired(long deadLine) {
        return triggerTime < deadLine;  // Return whether the row has expired based on the given deadline
    }

    @Override
    public long getSortKey() {
        return triggerTime;  // Return the trigger time as the sort key for ordering purposes
    }

    // Add a new column to the row with a specified name and value
    public void addCol(String colName, long value) {
        this.timeData.put(colName, value);  // Add the new column to the timeData map
    }

    // Calculate the process time of this row based on the earliest trigger time
    // This method is used to determine if the row is expired based on the oldest trigger time
    public long getProcessTime(long value) {
        long minTriggerTime = Long.MAX_VALUE;
        for (Map.Entry<String, Long> entry : this.timeData.entrySet()) {
            // Find the minimum trigger time across all columns
            if (entry.getKey().endsWith("_triggerTime") && entry.getValue() < minTriggerTime) {
                minTriggerTime = entry.getValue();
            }
        }
        return value - minTriggerTime;  // Return the difference between the provided value and the minimum trigger time
    }
}
