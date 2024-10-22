//package org.example.merger;
//
//import org.example.piepair.IEP;
//import org.example.piepair.eba.EBA;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//public class IERow {
//    private Map<String, Object> data;
//    private String joinKey;
//
////    // 默认构造函数
////    public IERow() {
////
////        this.data = new HashMap<>();
////        joinKey =null;
////    }
//
//    // 构造函数，接受列名和列值的 Map
//    public IERow(Map<String, Object> data, List<String> JoinedCols) {
//        this.data = data;
//        StringBuilder keyBuilder = new StringBuilder();
//        for (String   column : JoinedCols ){
//            keyBuilder.append(data.getOrDefault(column, "NULL")).append("|");
//        }
//        this.joinKey= keyBuilder.toString();
//    }
//
//    // 返回该行中某列的值
//    public Object getValue(String column) {
//        return data.get(column);
//    }
//
//    // 返回该行的所有列名
//    public Set<String> getColumnNames() {
//        return data.keySet();
//    }
//
////    // 根据多列生成哈希键，用于自然连接时的匹配
////    public String generateKey(Set<String> joinColumns) {
////        StringBuilder keyBuilder = new StringBuilder();
////        for (String column : joinColumns) {
////            keyBuilder.append(data.getOrDefault(column, "NULL")).append("|");
////        }
////        return keyBuilder.toString();
////    }
//
//    // 自然连接，将当前行与另一行进行连接
//    public IERow getJoinedRow(IERow other) {
//        Map<String, Object> joinedData = new HashMap<>(this.data);
//        joinedData.putAll(other.data);
//        return new IERow(joinedData);
//    }
//
//    // 打印行的内容
//    @Override
//    public String toString() {
//        return data.toString();
//    }
//
//
//
//}
