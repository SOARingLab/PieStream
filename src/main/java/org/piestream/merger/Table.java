package org.piestream.merger;

import org.piestream.engine.Window;
import org.piestream.engine.WindowType;
import org.piestream.piepair.IEP;
import org.piestream.piepair.eba.EBA;

import java.util.*;

public class Table {
    private final LinkList<Row> rows;
    private long size; // 当前行数
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


    // TODO: detele window
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
        if(otherTable==null){
            return;
        }
        if(tableJoinCols == addedTableJoinCols){
            concatenate(otherTable);
        }else{
            concatenateRebuildIndex(otherTable,tableJoinCols);
        }

    }

    //  有一些 concate 需要重建索引
    public void concatenateRebuildIndex(Table otherTable, List<String> newJoinColumns) {
        if(otherTable==null){
            return;
        }
        long CRstartTime= System.currentTimeMillis();
        if (otherTable.getSize() != 0) {
            LinkList<Row> otherRows = otherTable.getRows();

            LinkList<Row>.Node node=otherRows.getHead();
            while(node!=null){
                Row row=node.getData();
                addRow( new Row(row.getTimeData(),newJoinColumns,row.getSource(),row.getTriggerTime(), true )   );
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
