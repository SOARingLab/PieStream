package org.example.merger;

import org.example.piepair.IEP;
import org.example.piepair.eba.EBA;

import java.util.*;

public class Row {
    private Map<String, Long> data;
    private String indexKey;
    private Map<String, Long> col2Index;


//    // 默认构造函数
//    public Row() {
//        this.data = new HashMap<>();
//    }

    // 构造函数，接受列名和列值的 Map
    public Row(Map<String, Long> data, List<String> joinColumns) {
        this.data = data;
        this.indexKey=generateKey(joinColumns);
    }
    // 构造函数，接受列名和列值的 Map
    public Row(Map<String, Long> data, Set<String> acmltCols) {
        this.data = data;
        this.col2Index=new HashMap<>();
        generateKey(acmltCols);
    }

    public Row(Row row, List<String> joinColumns) {
        this.data = row.data;
        this.rebuildIndex(joinColumns);
    }

    public Row(Row row, Set<String> acmltCols) {
        this.data = row.data;
        this.col2Index.clear();
        generateKey(acmltCols);
    }

    public Row(Map<String, Long> data, Map<String, Long>  col2Index) {
        this.data = data;
        this.col2Index=col2Index;
    }


    // 构造函数，接受单个 IEP 和 EBA2String
    public Row(IEP iep, Map<EBA, String> EBA2String, List<String> joinColumns) {
        this(new HashMap<>(),new ArrayList<>());

        // 生成列名和列值
        String formerPieSTKey = EBA2String.get(iep.getFormerPie()) + ".ST";  // formerPieST 列名
        String latterPieSTKey = EBA2String.get(iep.getLatterPie()) + ".ST";  // latterPieST 列名
//        String formerPieETKey = EBA2String.get(iep.getFormerPie()) + ".ET";  // formerPieET 列名
//        String latterPieETKey = EBA2String.get(iep.getLatterPie()) + ".ET";  // latterPieET 列名
//        String relationKey = "r(" +EBA2String.get(iep.getFormerPie()) +"," +  EBA2String.get(iep.getLatterPie())+")";  // 关系列名

        // 将这些列和值放入 data 中
        this.data.put(formerPieSTKey, iep.getFormerStartTime() );
        this.data.put(latterPieSTKey, iep.getLatterStartTime() );
//        this.data.put(formerPieETKey, iep.getFormerEndTime() );
//        this.data.put(latterPieETKey, iep.getLatterEndTime() );
//        this.data.put(relationKey, iep.getRelation().toString());

        this.indexKey=generateKey(joinColumns);
    }


    // 构造函数，接受单个 IEP 和 EBA2String
    public Row(IEP iep, Map<EBA, String> EBA2String, Set<String> acmltCols) {
        this(new HashMap<>(),new HashSet<>());

        // 生成列名和列值
        String formerPieSTKey = EBA2String.get(iep.getFormerPie()) + ".ST";  // formerPieST 列名
        String latterPieSTKey = EBA2String.get(iep.getLatterPie()) + ".ST";  // latterPieST 列名
//        String formerPieETKey = EBA2String.get(iep.getFormerPie()) + ".ET";  // formerPieET 列名
//        String latterPieETKey = EBA2String.get(iep.getLatterPie()) + ".ET";  // latterPieET 列名
//        String relationKey = "r(" +EBA2String.get(iep.getFormerPie()) +"," +  EBA2String.get(iep.getLatterPie())+")";  // 关系列名

        // 将这些列和值放入 data 中
        this.data.put(formerPieSTKey, iep.getFormerStartTime() );
        this.data.put(latterPieSTKey, iep.getLatterStartTime() );
//        this.data.put(formerPieETKey, iep.getFormerEndTime() );
//        this.data.put(latterPieETKey, iep.getLatterEndTime() );
//        this.data.put(relationKey, iep.getRelation().toString());

         generateKey(acmltCols);
    }


    public void rebuildIndex(List<String> joinColumns){
        this.indexKey= generateKey(joinColumns);
    }

    // 返回该行中某列的值
    public Long getValue(String column) {
        return data.get(column);
    }

    public String getIndexKey(){
        return indexKey;
    }
    // 返回该行的所有列名
    public Set<String> getColumnNames() {
        return data.keySet();
    }

    // 根据多列生成哈希键，用于自然连接时的匹配
    private String generateKey(List<String> joinColumns) {
        if (joinColumns==null){//root 不需要
            return null;
        }
        StringBuilder keyBuilder = new StringBuilder();
        for (String column : joinColumns) {
            if(  column == null){
                throw new IllegalArgumentException("String parameter cannot be null");
            }
            keyBuilder.append(data.get (column+".ST" )).append("|");
        }
        return keyBuilder.toString();
    }
    // 根据多列生成哈希键，用于自然连接时的匹配
    private void generateKey(Set<String> acmltCols) {
        if (acmltCols!=null){//root 不需要
            for (String col:acmltCols){
                String colName=col+".ST";
                this.col2Index.put(colName,this.data.get(colName));
            }
        }
    }


    // 自然连接，将当前行与另一行进行连接
    public Row join(Row other, List<String> parentJoinColumns) {
        Map<String, Long> joinedData = new HashMap<>(this.data);
        joinedData.putAll(other.data);
        Row r= new Row(joinedData,parentJoinColumns);
        return r;
    }

    // 自然连接，将当前行与另一行进行连接
    public Row join(Row other, Set<String> parentJoinColumns) {
        Map<String, Long> joinedData = new HashMap<>(this.data);
        joinedData.putAll(other.data);
        Map<String, Long> joinedCol2Idx = new HashMap<>( );
        for (String col : parentJoinColumns){
            Long value= data.get(col)!=null?data.get(col):other.data.get(col);
            joinedCol2Idx.put(col,value);
        }
        Row r= new Row(joinedData,joinedCol2Idx);
        return r;
    }

    // 打印行的内容
    @Override
    public String toString() {
        return data.toString();
    }



}
