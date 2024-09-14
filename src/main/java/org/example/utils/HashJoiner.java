package org.example.utils;

import java.util.*;

public class HashJoiner {

    // 使用哈希连接两个表，基于多个 joinColumns 进行连接
    public static Table hashJoin(Table table1, Table table2) {
        // 获取两个表中列的交集
        Set<String> joinColumns = getJoinColumns(table1, table2);

        // 构建哈希索引
        Table smallerTable = table1.getRows().size() <= table2.getRows().size() ? table1 : table2;
        Table largerTable = table1.getRows().size() > table2.getRows().size() ? table1 : table2;
        Map<String, List<Row>> hashIndex = buildHashIndex(smallerTable, joinColumns);

        // 存储连接后的结果
        Table resultTable = new Table();

        // 遍历较大的表，查找匹配的行
        for (Row row : largerTable.getRows()) {
            String joinKey = row.generateKey(joinColumns);  // 基于多个列生成哈希键

            // 如果哈希表中存在匹配的行，则进行连接
            if (hashIndex.containsKey(joinKey)) {
                List<Row> matchingRows = hashIndex.get(joinKey);
                for (Row matchingRow : matchingRows) {
                    resultTable.addRow(row.join(matchingRow));  // 将连接后的行添加到结果表中
                }
            }
        }

        return resultTable;
    }

    // 获取两个表中的列名交集
    public static Set<String> getJoinColumns(Table table1, Table table2) {

        Set<String> table1Columns = table1.getColumnNames();
        Set<String> table2Columns = table2.getColumnNames();

        // 计算两个集合的交集
        table1Columns.retainAll(table2Columns);
        return table1Columns;
    }

    // 构建哈希索引，基于 joinColumns 列的组合值
    private static Map<String, List<Row>> buildHashIndex(Table table, Set<String> joinColumns) {
        Map<String, List<Row>> hashIndex = new HashMap<>();

        // 遍历表中的每一行，基于 joinColumns 列生成哈希键
        for (Row row : table.getRows()) {
            String joinKey = row.generateKey(joinColumns);
            hashIndex.computeIfAbsent(joinKey, k -> new ArrayList<>()).add(row);
        }

        return hashIndex;
    }

    public static void main(String[] args) {
        // 示例数据
        Map<String, String> row1 = new HashMap<>();
        row1.put("id", "1");
        row1.put("name", "Alice");
        row1.put("city", "Shanghai");
        row1.put("job", "2");

        Map<String, String> row2 = new HashMap<>();
        row2.put("id", "2");
        row2.put("name", "Bob");
        row2.put("job", "2");

        Map<String, String> row3 = new HashMap<>();
        row3.put("id", "1");
        row3.put("age", "25");
        row3.put("city", "Shanghai");
        row3.put("job", "2");

        Map<String, String> row4 = new HashMap<>();
        row4.put("id", "3");
        row4.put("age", "30");
        row4.put("city", "Beijing");
        row4.put("job", "2");

        // 构建表1
        Table table1 = new Table();
        table1.addRow(new Row(row1));
        table1.addRow(new Row(row2));

        // 构建表2
        Table table2 = new Table();
        table2.addRow(new Row(row3));
        table2.addRow(new Row(row4));

        // 使用哈希连接
        Table resultTable = HashJoiner.hashJoin(table1, table2);

        // 打印连接结果
        for (Row row : resultTable.getRows()) {
            System.out.println(row);
        }
    }
}
