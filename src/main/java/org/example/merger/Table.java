package org.example.utils;

import org.example.piepair.IEP;
import org.example.piepair.eba.EBA;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Table {
    private List<Row> rows;

    // 默认构造函数
    public Table() {
        this.rows = new ArrayList<>();
    }

    // 构造函数，接受 IEPCol 和 EBA2String，将每个 IEP 作为 Row 添加到表中
    public Table(LinkList<IEP> iepList, Map<EBA, String> EBA2String) {
        this.rows = new ArrayList<>();

        // 遍历 col.iepList 链表中的每个 IEP
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
        rows.add(row);
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

    public int getRowCount() {
        return rows.size();
    }
}
