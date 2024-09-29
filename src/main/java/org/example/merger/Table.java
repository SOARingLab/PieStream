package org.example.merger;

import org.example.piepair.IEP;
import org.example.piepair.eba.EBA;

import java.util.*;

public class Table {
    private List<Row> rows;
    private long capacity; // 表的最大容量
    private long size;     // 当前行数

    // 构造函数，接受容量参数
    public Table(long capacity) {
        this.rows = new ArrayList<>();
        this.capacity = capacity;
        this.size = 0;
    }

    // 构造函数，接受 IEP 列表和 EBA2String 映射，以及容量参数
    public Table(LinkList<IEP> iepList, Map<EBA, String> EBA2String ) {
        this.rows = new ArrayList<>();
        this.capacity = iepList.getSize();
        this.size = 0;

        // 遍历 iepList 链表中的每个 IEP
        LinkList<IEP>.Node current = iepList.getHead(); // 获取链表的头节点
        while (current != null) {
            IEP iep = current.data; // 获取当前节点的 IEP

            // 使用 Row(IEP iep, Map<EBA, String> EBA2String) 构造 Row
            Row row = new Row(iep, EBA2String);

            // 添加到 Table 中
            this.addRow(row);

            // 移动到下一个节点
            current = current.next;
        }
    }



    // 添加一行到表中
    public void addRow(Row row) {
        // 检查容量是否已满
        if (size >= capacity) {
            // 容量已满，删除最旧的行（第一个元素）
            rows.remove(0);
            size--;
        }
        // 添加新行
        rows.add(row);
        size++;
    }

    // 返回表的所有行
    public List<Row> getRows() {
        return rows;
    }

    // 返回表中某列的所有值
    public Set<String> getColumnNames() {
        if (!rows.isEmpty()) {
            return rows.get(0).getColumnNames();
        }
        return null;
    }

    // 打印表的所有行
    public void printTable() {
        if (rows.isEmpty()) {
            System.out.println("The table is empty.");
        } else {
            for (Row row : rows) {
                System.out.println(row.toString()); // 打印每一行
            }
        }
    }

    // Print all rows of the table with aligned columns
    public void printTableFormat() {
        if (rows.isEmpty()) {
            System.out.println("The table is empty.");
        } else {
            // Get the column names in order
            List<String> columnNames = new ArrayList<>(getColumnNames());

            // Calculate the maximum width for each column
            Map<String, Integer> columnWidths = new LinkedHashMap<>();
            for (String columnName : columnNames) {
                int maxWidth = columnName.length(); // Start with the length of the header
                for (Row row : rows) {
                    String value = row.getValue(columnName);
                    if (value != null && value.length() > maxWidth) {
                        maxWidth = value.length();
                    }
                }
                columnWidths.put(columnName, maxWidth);
            }

            // Build the format string for each column
            StringBuilder formatBuilder = new StringBuilder();
            for (String columnName : columnNames) {
                int width = columnWidths.get(columnName) + 2; // Add padding
                formatBuilder.append("%-").append(width).append("s");
            }
            String format = formatBuilder.toString().trim(); // Remove trailing spaces

            // Print the header
            Object[] headerValues = columnNames.toArray();
            System.out.printf(format + "%n", headerValues);

            // Print each row
            for (Row row : rows) {
                List<String> values = new ArrayList<>();
                for (String columnName : columnNames) {
                    String value = row.getValue(columnName);
                    values.add(value != null ? value : ""); // Handle null values
                }
                System.out.printf(format + "%n", values.toArray());
            }
        }
    }



    // 返回表的当前行数
    public long getRowCount() {
        return size;
    }

    // 返回表的容量
    public long getCapacity() {
        return capacity;
    }
    // 合并另一个 Table 的所有行到当前 Table
    public void concatenate(Table otherTable) {
        if(otherTable.getRowCount()!=0){
            List<Row> otherRows = otherTable.getRows();
            for (Row row : otherRows) {
                this.addRow(row);
            }
        }
    }
    // 清空表中的所有行
    public void clear() {
        rows.clear(); // 清空行列表
        size = 0;     // 重置当前行数
    }
}
