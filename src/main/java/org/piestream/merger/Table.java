package org.piestream.merger;

import org.piestream.engine.Window;
import org.piestream.engine.WindowType;
import org.piestream.piepair.IEP;
import org.piestream.piepair.eba.EBA;

import java.util.*;

public class Table {
//    private final List<Row> rows;
    private final LinkList<Row> rows;
//    private final long  capacity; // 表的最大容量
    private long size; // 当前行数
//    private final WindowType winType; // 当前行数
    private final Window window;
    private final Map<String, List<Row>> hashIndex;

    public static long removeRowsAndIndexTime;
    public static long concateTime;
    public static long concateRebuilTime;
    public static long addRowsTime;
    public static long clearRowsTime;
    public static long addRowMergeTime;
    public static long startTime;
    public static long endTime;

//    // 构造函数，接受容量参数
//    public Table(long capacity) {
////        this.rows = new ArrayList<>();
//        this.winType=WindowType.COUNT_WINDOW;
//        this.capacity = capacity;
//        this.size = 0;
//        this.hashIndex=new HashMap<>();
//        this.rows =new LinkList<Row>(capacity);
//    }

    public Table(Window window ) {
        this.window = window;
        this.size = 0;
        this.hashIndex=new HashMap<>();
        this.rows =new LinkList<Row>(window);
    }


    public void update(String pred,Long startTime,Long endTime){

        LinkList<Row>.Node node=rows.getHead();
        while(node!=null){
            Row row=node.getData();
            String searchColName=pred+".ST";
            if(row.getValueFromColName(searchColName)==startTime){
                row.update(pred+".ET",endTime);
            } else if (row.getValueFromColName(searchColName)<startTime) {
                node=node.next;
            }else{
                return;
            }
        }
    }

    public  Map<String, List<Row>> getHashIndex(){
        return hashIndex;
    }

    //COUNT_WINDOW
    private void deleteRowsAndIndex(long toDelSize){
        if (toDelSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Excess size exceeds Integer.MAX_VALUE, cannot proceed.");
        }
        if(toDelSize> window.getWindowCapacity() ){
            toDelSize=window.getWindowCapacity();
        }
        int cnt=0;
        while(cnt<toDelSize ){
            Row oldestRow= this.rows.deleteHead();
            deleteHashindexByRow(oldestRow);
            cnt++;
        }
        size -= toDelSize;
    }

    private void deleteHashindexByRow(Row row){
        // 更新 hashIndex 中的索引
        String indexKey = row.getIndexKey();
        List<Row> sameIndexRowList = hashIndex.get(indexKey);
        if (sameIndexRowList != null) {
            sameIndexRowList.remove(row);
            if (sameIndexRowList.isEmpty()) {
                hashIndex.remove(indexKey);
            }
        }
    }

    public void addRowsWithoutGenIndex(LinkList<Row> rowsToAdd) {
        long excess = size + rowsToAdd.getSize()  - this.getCapacity();

        if (window.getWindowType()==WindowType.CAPACITY_WINDOW && excess > 0) {
            startTime= System.currentTimeMillis();
            deleteRowsAndIndex(excess);
            endTime= System.currentTimeMillis();
            removeRowsAndIndexTime+=(endTime-startTime);
        }

        startTime= System.currentTimeMillis();
        // 批量添加新行
        rows.concat(rowsToAdd);
        size += rowsToAdd.getSize() ;
        endTime= System.currentTimeMillis();
        addRowsTime+=(endTime-startTime);
    }

    // 添加一行到表中
    public void addRow(Row row) {
        // 检查容量是否已满
        if (window.getWindowType()==WindowType.CAPACITY_WINDOW &&size >= this.getCapacity()) {
           deleteRowsAndIndex(1);
        }

        // 添加新行
        rows.safeAdd(row);
        size++;

        // 更新 hashIndex
        String newIndexKey = row.getIndexKey();
        if(  newIndexKey ==""){
            throw new IllegalArgumentException("String parameter cannot be null");
        }
        hashIndex.computeIfAbsent(newIndexKey, k -> new ArrayList<>()).add(row);
    }


    public void addRow(IEP iep, Map<EBA, String> EBA2String, List<String> joinColumns) {
        addRow(new Row(iep, EBA2String,joinColumns)  );
    }



    // 返回表的所有行
    public LinkList<Row> getRows() {
        return rows;
    }

//    // 返回表中某列的所有值
//    public Set<String> getColumnNames() {
//        if (!rows.isEmpty()) {
//            return rows.get(0).getColumnNames();
//        }
//        return null;
//    }
//
//    // 打印表的所有行
//    public void printTable() {
//        if (rows.isEmpty()) {
//            System.out.println("The table is empty.");
//        } else {
//            for (Row row : rows) {
//                System.out.println(row.toString()); // 打印每一行
//            }
//        }
//    }
//
//    // Print all rows of the table with aligned columns
//    public void printTableFormat() {
//        if (rows.isEmpty()) {
//            System.out.println("The table is empty.");
//        } else {
//            // Get the column names in order
//            List<String> columnNames = new ArrayList<>(getColumnNames());
//
//            // Calculate the maximum width for each column
//            Map<String, Integer> columnWidths = new LinkedHashMap<>();
//            for (String columnName : columnNames) {
//                int maxWidth = columnName.length(); // Start with the length of the header
//                for (Row row : rows) {
//                    String value = row.getValue(columnName).toString();
//                    if (value != null && value.length() > maxWidth) {
//                        maxWidth = value.length();
//                    }
//                }
//                columnWidths.put(columnName, maxWidth);
//            }
//
//            // Build the format string for each column
//            StringBuilder formatBuilder = new StringBuilder();
//            for (String columnName : columnNames) {
//                int width = columnWidths.get(columnName) + 2; // Add padding
//                formatBuilder.append("%-").append(width).append("s");
//            }
//            String format = formatBuilder.toString().trim(); // Remove trailing spaces
//
//            // Print the header
//            Object[] headerValues = columnNames.toArray();
//            System.out.printf(format + "%n", headerValues);
//
//            // Print each row
//            for (Row row : rows) {
//                List<String> values = new ArrayList<>();
//                for (String columnName : columnNames) {
//                    String value = row.getValue(columnName).toString();
//                    values.add(value != null ? value : ""); // Handle null values
//                }
//                System.out.printf(format + "%n", values.toArray());
//            }
//        }
//    }
//
//    public void printTableOrdered() {
//        if (rows.isEmpty()) {
//            System.out.println("The table is empty.");
//        } else {
//            // Get the column names
//            Set<String> columnNameSet = getColumnNames();
//
//            // Identify stCols and otherCols
//            List<String> stCols = new ArrayList<>();
//            List<String> otherCols = new ArrayList<>();
//
//            for (String colName : columnNameSet) {
//                if (colName.endsWith(".ST")) {
//                    stCols.add(colName);
//                } else {
//                    otherCols.add(colName);
//                }
//            }
//
//            // Sort stCols according to the character before ".ST"
//            stCols.sort(new Comparator<String>() {
//                @Override
//                public int compare(String s1, String s2) {
//                    String c1 = s1.substring(0, s1.length() - 3); // Remove ".ST"
//                    String c2 = s2.substring(0, s2.length() - 3);
//                    return c1.compareTo(c2);
//                }
//            });
//
//            // Sort the rows according to the values in stCols
//            Collections.sort(rows, new Comparator<Row>() {
//                @Override
//                public int compare(Row r1, Row r2) {
//                    for (String col : stCols) {
//                        String v1 = r1.getValue(col).toString();
//                        String v2 = r2.getValue(col).toString();
//                        int cmp = compareValues(v1, v2);
//                        if (cmp != 0) {
//                            return cmp;
//                        }
//                    }
//                    return 0;
//                }
//
//                private int compareValues(String v1, String v2) {
//                    // Handle nulls
//                    if (v1 == null && v2 == null) {
//                        return 0;
//                    }
//                    if (v1 == null) {
//                        return -1;
//                    }
//                    if (v2 == null) {
//                        return 1;
//                    }
//                    // Try to parse as numbers
//                    try {
//                        double d1 = Double.parseDouble(v1);
//                        double d2 = Double.parseDouble(v2);
//                        return Double.compare(d1, d2);
//                    } catch (NumberFormatException e) {
//                        // Compare as strings
//                        return v1.compareTo(v2);
//                    }
//                }
//            });
//
//            // Combine stCols and otherCols
//            List<String> columnNames = new ArrayList<>();
//            columnNames.addAll(stCols);
//            columnNames.addAll(otherCols);
//
//            // Calculate the maximum width for each column
//            Map<String, Integer> columnWidths = new LinkedHashMap<>();
//            for (String columnName : columnNames) {
//                int maxWidth = columnName.length(); // Start with the length of the header
//                for (Row row : rows) {
//                    String value = row.getValue(columnName).toString();
//                    if (value != null && value.length() > maxWidth) {
//                        maxWidth = value.length();
//                    }
//                }
//                columnWidths.put(columnName, maxWidth);
//            }
//
//            // Build the format string for each column
//            StringBuilder formatBuilder = new StringBuilder();
//            for (String columnName : columnNames) {
//                int width = columnWidths.get(columnName) + 2; // Add padding
//                formatBuilder.append("%-").append(width).append("s");
//            }
//            String format = formatBuilder.toString().trim(); // Remove trailing spaces
//
//            // Print the header
//            Object[] headerValues = columnNames.toArray();
//            System.out.printf(format + "%n", headerValues);
//
//            // Print each row
//            for (Row row : rows) {
//                List<String> values = new ArrayList<>();
//                for (String columnName : columnNames) {
//                    String value = row.getValue(columnName).toString();
//                    values.add(value != null ? value : ""); // Handle null values
//                }
//                System.out.printf(format + "%n", values.toArray());
//            }
//        }
//    }

    // 返回表的当前行数
    public long getSize() {
        return size;
    }

    // 返回表的容量
    public long getCapacity() {
        return window.getWindowCapacity();
    }

    public Window getWindow(){
        return window;
    }


    public void concatenate(Table otherTable ) {
        if(otherTable==null){
            return;
        }
        long concatST=System.currentTimeMillis();
        if (otherTable.getSize() != 0) {
            LinkList<Row> otherRows = otherTable.getRows();
            // Pre-allocate capacity if using ArrayList

            // Add all rows from otherTable
            long addRowMergeST=System.currentTimeMillis();
            addRowsWithoutGenIndex(otherRows);
            MapMerger.mergeSimpleMaps(hashIndex,otherTable.getHashIndex() );
            long addRowMergeET= System.currentTimeMillis();
            addRowMergeTime+=(addRowMergeET-addRowMergeST);
        }
        long concatET= System.currentTimeMillis();
        concateTime+=(concatET-concatST);
    }



    public void concatenate(Table otherTable, List<String> tableJoinCols,List<String> addedTableJoinCols) {

        if(tableJoinCols == addedTableJoinCols){
            concatenate(otherTable);
        }else{
            concatenateRebuildIndex(otherTable,tableJoinCols);
        }

    }

    //  有一些 concate 需要重建索引
    public void concatenateRebuildIndex(Table otherTable, List<String> newJoinColumns) {
        long CRstartTime= System.currentTimeMillis();
        if (otherTable.getSize() != 0) {
            LinkList<Row> otherRows = otherTable.getRows();

            LinkList<Row>.Node node=otherRows.getHead();
            while(node!=null){
                Row row=node.getData();
                addRow( new Row(row.getTimeData(),newJoinColumns,row.getIepSource(),row.getTriggerTime(), true )   );
                node=node.next;
            }
        }

        long CRendTime= System.currentTimeMillis();
        concateRebuilTime+=(CRendTime-CRstartTime);
    }

    public long addDetectTimeAndCalProcessTime(String colName,long value){
        LinkList<Row>.Node nd= rows.getHead();
        long accumlatedProcessTime=0;
        while(nd!=null){

            nd.getData().addCol(colName,value);
            accumlatedProcessTime+=nd.getData().getProcessTime(value);
            nd=nd.next;
        }
        return accumlatedProcessTime;
    }

    // 清空表中的所有行
    public void clear() {
        rows.clear(); // 清空行列表
        size = 0; // 重置当前行数
        hashIndex.clear();
    }
    public void refreshIndex(long deadLine, List<Row> toDelRows){
        for(Row row:toDelRows){
            deleteHashindexByRow(row);
        }
    }
    public void  refresh(long deadLine ){
        List<Row> toDelRows=rows.refresh(deadLine);
        size-=(toDelRows.size()) ;
        refreshIndex(deadLine,toDelRows);

    }

}
