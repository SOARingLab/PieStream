package org.example.merger;

import org.example.piepair.IEP;
import org.example.piepair.eba.EBA;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Row {
    private Map<String, String> data;

    // 默认构造函数
    public Row() {
        this.data = new HashMap<>();
    }

    // 构造函数，接受列名和列值的 Map
    public Row(Map<String, String> data) {
        this.data = data;
    }

    // 返回该行中某列的值
    public String getValue(String column) {
        return data.get(column);
    }

    // 返回该行的所有列名
    public Set<String> getColumnNames() {
        return data.keySet();
    }

    // 根据多列生成哈希键，用于自然连接时的匹配
    public String generateKey(Set<String> joinColumns) {
        StringBuilder keyBuilder = new StringBuilder();
        for (String column : joinColumns) {
            keyBuilder.append(data.getOrDefault(column, "NULL")).append("|");
        }
        return keyBuilder.toString();
    }

    // 自然连接，将当前行与另一行进行连接
    public Row join(Row other) {
        Map<String, String> joinedData = new HashMap<>(this.data);
        joinedData.putAll(other.data);
        return new Row(joinedData);
    }

    // 打印行的内容
    @Override
    public String toString() {
        return data.toString();
    }

    // 构造函数，接受单个 IEP 和 EBA2String
    public Row(IEP iep, Map<EBA, String> EBA2String) {
        this.data = new HashMap<>();

        // 生成列名和列值
        String formerPieSTKey = EBA2String.get(iep.getFormerPie()) + ".ST";  // formerPieST 列名
        String latterPieSTKey = EBA2String.get(iep.getLatterPie()) + ".ST";  // latterPieST 列名
        String formerPieETKey = EBA2String.get(iep.getFormerPie()) + ".ET";  // formerPieET 列名
        String latterPieETKey = EBA2String.get(iep.getLatterPie()) + ".ET";  // latterPieET 列名
        String relationKey = "r(" +EBA2String.get(iep.getFormerPie()) +"," +  EBA2String.get(iep.getLatterPie())+")";  // 关系列名

        // 将这些列和值放入 data 中
        data.put(formerPieSTKey, iep.getFormerStartTime().toString());
        data.put(latterPieSTKey, iep.getLatterStartTime().toString());
        data.put(formerPieETKey, iep.getFormerEndTime() );
        data.put(latterPieETKey, iep.getLatterEndTime() );
        data.put(relationKey, iep.getRelation().toString());
    }

}
