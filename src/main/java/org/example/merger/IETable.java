//package org.example.merger;
//
//import org.example.piepair.IEP;
//import org.example.piepair.eba.EBA;
//
//import java.util.*;
//
//public class IETable {
//    private LinkList<IERow> rows;
//    private long capacity; // 表的最大容量
//    private long size; // 当前行数
//
//    // 构造函数，接受容量参数
//    public IETable(long capacity) {
//        this.rows = new LinkList<IERow>(capacity);
//        this.capacity = capacity;
//        this.size = 0;
//    }
//
//    // 构造函数，接受 IEP 列表和 EBA2String 映射，以及容量参数
//    public IETable(long capacity,LinkList<IEP> iepList, Map<EBA, String> EBA2String) {
//        this.rows = new LinkList<IERow>(capacity);
//        this.capacity = capacity;
//        this.size = 0;
//
//        // 遍历 iepList 链表中的每个 IEP
//        LinkList<IEP>.Node current = iepList.getHead(); // 获取链表的头节点
//        while (current != null) {
//            IEP iep = current.data; // 获取当前节点的 IEP
//
//            // 使用 IERow(IEP iep, Map<EBA, String> EBA2String) 构造 IERow
//            IERow row = new IERow(iep, EBA2String);
//
//            // 添加到 IETable 中
//            this.addIERow(row);
//
//            // 移动到下一个节点
//            current = current.next;
//        }
//    }
//
//    // 添加一行到表中
//    public void addIERow(IERow row) {
//        // 检查容量是否已满
//        if (size >= capacity) {
//            // 容量已满，删除最旧的行（第一个元素）
//            rows.remove(0);
//            size--;
//        }
//        // 添加新行
//        rows.add(row);
//        size++;
//    }
//
//    // 返回表的所有行
//    public List<IERow> getIERows() {
//        return rows;
//    }
//
//    // 返回表中某列的所有值
//    public Set<String> getColumnNames() {
//        if (!rows.isEmpty()) {
//            return rows.get(0).getColumnNames();
//        }
//        return null;
//    }
//
//    // 打印表的所有行
//    public void printIETable() {
//        if (rows.isEmpty()) {
//            System.out.println("The table is empty.");
//        } else {
//            for (IERow row : rows) {
//                System.out.println(row.toString()); // 打印每一行
//            }
//        }
//    }
//
//    // Print all rows of the table with aligned columns
//    public void printIETableFormat() {
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
//                for (IERow row : rows) {
//                    String value = row.getValue(columnName);
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
//            for (IERow row : rows) {
//                List<String> values = new ArrayList<>();
//                for (String columnName : columnNames) {
//                    String value = row.getValue(columnName);
//                    values.add(value != null ? value : ""); // Handle null values
//                }
//                System.out.printf(format + "%n", values.toArray());
//            }
//        }
//    }
//
//    public void printIETableOrdered() {
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
//            Collections.sort(rows, new Comparator<IERow>() {
//                @Override
//                public int compare(IERow r1, IERow r2) {
//                    for (String col : stCols) {
//                        String v1 = r1.getValue(col);
//                        String v2 = r2.getValue(col);
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
//                for (IERow row : rows) {
//                    String value = row.getValue(columnName);
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
//            for (IERow row : rows) {
//                List<String> values = new ArrayList<>();
//                for (String columnName : columnNames) {
//                    String value = row.getValue(columnName);
//                    values.add(value != null ? value : ""); // Handle null values
//                }
//                System.out.printf(format + "%n", values.toArray());
//            }
//        }
//    }
//
//    // 返回表的当前行数
//    public long getIERowCount() {
//        return size;
//    }
//
//    // 返回表的容量
//    public long getCapacity() {
//        return capacity;
//    }
//
//    // 合并另一个 IETable 的所有行到当前 IETable
//    // public void concatenate(IETable otherIETable) {
//    // if(otherIETable.getIERowCount()!=0){
//    // List<IERow> otherIERows = otherIETable.getIERows();
//    // for (IERow row : otherIERows) {
//    // this.addIERow(row);
//    // }
//    // }
//    // }
//
//    public void concatenate(IETable otherIETable) {
//        if (otherIETable.getIERowCount() != 0) {
//            List<IERow> otherIERows = otherIETable.getIERows();
//            // Pre-allocate capacity if using ArrayList
//            if (rows instanceof ArrayList) {
//                ((ArrayList<IERow>) rows).ensureCapacity((int) (size + otherIERows.size()));
//            }
//            // Add all rows from otherIETable
//            rows.addAll(otherIERows);
//            size += otherIERows.size();
//        }
//    }
//
//    // 清空表中的所有行
//    public void clear() {
//        rows.clear(); // 清空行列表
//        size = 0; // 重置当前行数
//    }
//
//    public void concatenateList(LinkList<IEP> iepList, Map<EBA, String> EBA2String) {
//        if (iepList != null && iepList.getSize() > 0) {
//            // Pre-allocate capacity if using ArrayList
//            if (rows instanceof ArrayList) {
//                ((ArrayList<IERow>) rows).ensureCapacity((int) (size + iepList.getSize()));
//            }
//
//            // Traverse the IEP list and convert each IEP to a IERow
//            LinkList<IEP>.Node current = iepList.getHead();
//            while (current != null) {
//                IEP iep = current.data;
//                IERow row = new IERow(iep, EBA2String);
//                rows.add(row);
//                size++;
//                current = current.next;
//            }
//        }
//    }
//}
