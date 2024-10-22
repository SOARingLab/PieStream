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

        // 遍历较大的表，查找匹配的行
        for (Row row : largerTable.getRows()) {
            String joinKey = row.getIndexKey();  // 基于多个列生成哈希键

            // 如果哈希表中存在匹配的行，则进行连接
            if (hashIndex.containsKey(joinKey)) {
                List<Row> matchingRows = hashIndex.get(joinKey);
                for (Row matchingRow : matchingRows) {
                    resultTable.addRow( row.join(matchingRow,parentJoinColumns));  // 将连接后的行添加到结果表中
                }
            }
        }

        return resultTable;
    }

//    // 获取两个表中的列名交集
//    public static Set<String> getJoinColumns(Table table1, Table table2) {
//
//        Set<String> cols= new HashSet<>();
////        Set<String> table1Columns = table1.getColumnNames();
//        // 获取 table1 中以 "ST" 结尾的列名
//        Set<String> table1Columns = table1.getColumnNames().stream()
//                .filter(column -> column.endsWith("ST"))
//                .collect(Collectors.toSet());
//        Set<String> table2Columns = table2.getColumnNames();
//
//        // 计算两个集合的交集
//        cols.addAll(table1Columns);
//        cols.retainAll(table2Columns);
//        return cols;
//    }

    // 构建哈希索引，基于 joinColumns 列的组合值
//    private static Map<String, List<Row>> buildHashIndex(Table table, Set<String> joinColumns) {
//        Map<String, List<Row>> hashIndex = new HashMap<>();
//
//        // 遍历表中的每一行，基于 joinColumns 列生成哈希键
//        for (Row row : table.getRows()) {
//            String joinKey = row.generateKey(joinColumns);
//            hashIndex.computeIfAbsent(joinKey, k -> new ArrayList<>()).add(row);
//        }
//
//        return hashIndex;
//    }

}
