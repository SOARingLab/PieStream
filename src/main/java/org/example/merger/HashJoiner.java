package org.example.merger;

import java.util.*;
import java.util.stream.Collectors;

public class HashJoiner {

    // 使用哈希连接两个表，基于多个 joinColumns 进行连接
    public static Table hashJoin(Table table1, Table table2, List<String> parentJoinColumns) {

        if(table1.getRowCount() * table2.getRowCount()==0){
            return new Table(0);
        }
//        // 获取两个表中列的交集
//        Set<String> joinColumns = getJoinColumns(table1, table2);

        // 构建哈希索引
        Table smallerTable = table1.getRows().size() <= table2.getRows().size() ? table1 : table2;
        Table largerTable = table1.getRows().size() > table2.getRows().size() ? table1 : table2;
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

    // 使用哈希连接两个表，基于多个 joinColumns 进行连接
    public static Table hashJoin(Table table1, Table table2,Set<String> joinedCols,Set<String> parentJoinColumns) {

        if(table1.getRowCount() * table2.getRowCount()==0){
            return new Table(0);
        }
//        // 获取两个表中列的交集
//        Set<String> joinColumns = getJoinColumns(table1, table2);

        // 构建哈希索引
        Table smallerTable = table1.getRows().size() <= table2.getRows().size() ? table1 : table2;
        Table largerTable = table1.getRows().size() > table2.getRows().size() ? table1 : table2;
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
