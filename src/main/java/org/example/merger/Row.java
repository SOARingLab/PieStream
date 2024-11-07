package org.example.merger;

import org.example.events.Expirable;
import org.example.piepair.IEP;
import org.example.piepair.eba.EBA;

import java.util.*;

public class Row implements Expirable {
    private Map<String, Long> data;
    private long triggerTime  ;
    private String indexKey;


    // 构造函数，接受列名和列值的 Map
    public Row(Map<String, Long> data, List<String> joinColumns,  boolean needNewIndex) {

        this.data = data;
        if(needNewIndex){
            this.indexKey=generateKey(joinColumns);
        }else{
            this.indexKey=null; // ROOT 的情况，最上节点不需要index
        }

    }
    // 构造函数，接受列名和列值的 Map
    public Row(Map<String, Long> data, List<String> joinColumns,long triggerTime , boolean needNewIndex) {
        this.triggerTime=triggerTime;
        this.data = data;
        if(needNewIndex){
            this.indexKey=generateKey(joinColumns);
        }else{
            this.indexKey=null;
        }

    }


    // 构造函数，接受单个 IEP 和 EBA2String
    public Row(IEP iep, Map<EBA, String> EBA2String, List<String> joinColumns) {

        // 生成列名和列值
        String formerPieSTKey = EBA2String.get(iep.getFormerPie()) + ".ST";  // formerPieST 列名
        String latterPieSTKey = EBA2String.get(iep.getLatterPie()) + ".ST";  // latterPieST 列名
        String formerPieETKey = EBA2String.get(iep.getFormerPie()) + ".ET";  // formerPieET 列名
        String latterPieETKey = EBA2String.get(iep.getLatterPie()) + ".ET";  // latterPieET 列名
//        String relationKey = "r(" +EBA2String.get(iep.getFormerPie()) +"," +  EBA2String.get(iep.getLatterPie())+")";  // 关系列名


        this.data = new HashMap<>();
        this.data.put(formerPieSTKey, iep.getFormerStartTime() );
        this.data.put(latterPieSTKey, iep.getLatterStartTime() );
        this.data.put(formerPieETKey, iep.getFormerEndTime() );
        this.data.put(latterPieETKey, iep.getLatterEndTime() );
//        this.data.put(relationKey, iep.getRelation().toString());

        this.triggerTime=iep.getTriggerTime();
        this.indexKey=generateKey(joinColumns);
    }

    public long getTriggerTime() {
        return triggerTime;
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
            throw new IllegalArgumentException("String parameter cannot be null");
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



    // 自然连接，将当前行与另一行进行连接
    public Row join(Row other, List<String> parentJoinColumns,boolean needNewIndex) {
        long lastTriggerTime=this.triggerTime>=other.triggerTime?this.triggerTime:other.triggerTime;
        Map<String, Long> joinedData = new HashMap<>(this.data);
        joinedData.putAll(other.data);
        Row r= new Row(joinedData,parentJoinColumns,lastTriggerTime,needNewIndex);
        return r;
    }


    public Map<String, Long> getData() {
        return data;
    }

    // 打印行的内容
    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public  boolean isExpired(long deadLine){
        return triggerTime<deadLine;
    }

    @Override
    public long getSortKey() {
        return triggerTime;
    }
}
