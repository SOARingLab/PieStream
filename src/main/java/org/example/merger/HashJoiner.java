package org.example.merger;

import java.util.*;
import java.util.stream.Collectors;

public class HashJoiner {
    public static long searchForJoin=0;
    public static long startTime=0;
    public static long endTime=0;

    public static long joinCNT=0;
    // 使用哈希连接两个表，基于多个 joinColumns 进行连接
    public static Table hashJoin(Table table1, Table table2, List<String> parentJoinColumns,boolean needNewIndex) {

        if(table1.getRowCount() * table2.getRowCount()==0){
            return new Table(0);
        }

        Table smallerTable = table1.getRows().getSize() <= table2.getRows().getSize() ? table1 : table2;
        Table largerTable = table1.getRows().getSize() > table2.getRows().getSize() ? table1 : table2;

        startTime = System.currentTimeMillis();

//        Table resultTable=JoinTwoTableNaive(smallerTable,largerTable,parentJoinColumns,needNewIndex);
        Table resultTable=JoinTwoTableQuick(smallerTable,largerTable,parentJoinColumns,needNewIndex);

        endTime = System.currentTimeMillis();
        searchForJoin += (endTime - startTime);
        return resultTable;
    }

    public static Table JoinTwoTableQuick(Table smallerTable,Table largerTable,List<String> parentJoinColumns,boolean needNewIndex){
        long capacity= smallerTable.getCapacity()>largerTable.getCapacity()?smallerTable.getCapacity():largerTable.getCapacity();
        // 存储连接后的结果
        Table resultTable = new Table(capacity);

        Map<String, List<Row>> smallHashIndex = smallerTable.getHashIndex();
        Map<String, List<Row>> largeHashIndex = largerTable.getHashIndex();
        Set <String > sharedIndex=new HashSet<>(smallHashIndex.keySet());
        sharedIndex.retainAll(largeHashIndex.keySet());


        for (  String indexKey:sharedIndex){
            for (Row largeRow: largeHashIndex.get(indexKey)){
                for(Row smallRow: smallHashIndex.get(indexKey)){
                    resultTable.addRow( largeRow.join(smallRow,parentJoinColumns, needNewIndex));
                    joinCNT++;
                }
            }
        }
        return resultTable;
    }

    public static Table JoinTwoTableNaive(Table smallerTable,Table largerTable,List<String> parentJoinColumns,boolean needNewIndex){

        Map<String, List<Row>> hashIndex = smallerTable.getHashIndex();
//        Map<String, List<Row>> largeHashIndex = smallerTable.getHashIndex();

        long capacity= smallerTable.getCapacity()>largerTable.getCapacity()?smallerTable.getCapacity():largerTable.getCapacity();
        // 存储连接后的结果
        Table resultTable = new Table(capacity);
        for (  String indexKey:largerTable.getHashIndex().keySet()){
            if (hashIndex.containsKey(indexKey)) {
                for (Row row: largerTable.getHashIndex().get(indexKey)){
                    List<Row> matchingRows = hashIndex.get(indexKey);
                    for (Row matchingRow : matchingRows) {
                        resultTable.addRow( row.join(matchingRow,parentJoinColumns, needNewIndex));  // 将连接后的行添加到结果表中
                    }
                }

            }
        }
        return resultTable;
    }

    // 使用哈希连接两个表，基于多个 joinColumns 进行连接
    public static Table hashJoin(Table table1, Table table2,Set<String> joinedCols,Set<String> parentJoinColumns) {

        if(table1.getRowCount() * table2.getRowCount()==0){
            return new Table(0);
        }
//        // 获取两个表中列的交集
//        Set<String> joinColumns = getJoinColumns(table1, table2);

        // 构建哈希索引
        Table smallerTable = table1.getRows().getSize() <= table2.getRows().getSize() ? table1 : table2;
        Table largerTable = table1.getRows().getSize() > table2.getRows().getSize() ? table1 : table2;
//        Map<String, List<Row>> hashIndex = buildHashIndex(smallerTable, joinColumns);

        Map<String, List<Row>> hashIndex = smallerTable.getHashIndex();

        long capacity= table1.getCapacity()>table2.getCapacity()?table1.getCapacity():table2.getCapacity();
        // 存储连接后的结果
        Table resultTable = new Table(capacity);


        for (  String indexKey:largerTable.getHashIndex().keySet()){
            if (hashIndex.containsKey(indexKey)) {
                for (Row row: largerTable.getHashIndex().get(indexKey)){
                    List<Row> matchingRows = hashIndex.get(indexKey);
                    for (Row matchingRow : matchingRows) {
                        resultTable.addRow( row.join(matchingRow,parentJoinColumns));  // 将连接后的行添加到结果表中
                    }
                }

            }
        }

        return resultTable;
    }


}
