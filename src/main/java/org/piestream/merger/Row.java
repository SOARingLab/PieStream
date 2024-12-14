package org.piestream.merger;

import org.piestream.events.Expirable;
import org.piestream.piepair.IEP;
import org.piestream.piepair.eba.EBA;

import java.util.*;

public class Row implements Expirable {
    private Map<String, Long> timeData;
    private Set<IEP> iepSource;
    private long triggerTime;   //  iep中最晚的 triggerTime，决定了这个row的过期时间
    private String indexKey;    //  这个row的index



    // 构造函数，接受列名和列值的 Map
    public Row(Map<String, Long> timeData, List<String> joinColumns, Set<IEP> iepSource, long triggerTime , boolean needNewIndex) {
        this.triggerTime=triggerTime;
        this.timeData = timeData;
        this.iepSource=iepSource;
        if(needNewIndex){    // 非 root 节点都需要 index
            this.indexKey=generateKey(joinColumns);
        }else{  // root 节点不需要 index
            this.indexKey=null;
        }
    }


    // 从叶子节点构造Row，仅包含一个iep信息
    public Row(IEP iep, Map<EBA, String> EBA2String, List<String> joinColumns) {

        // 生成列名和列值
        String formerPieSTKey = EBA2String.get(iep.getFormerPie()) + ".ST";  // formerPieST 列名
        String latterPieSTKey = EBA2String.get(iep.getLatterPie()) + ".ST";  // latterPieST 列名
//        String formerPieETKey = EBA2String.get(iep.getFormerPie()) + ".ET";  // formerPieET 列名
//        String latterPieETKey = EBA2String.get(iep.getLatterPie()) + ".ET";  // latterPieET 列名
//        String relationKey = "r(" +EBA2String.get(iep.getFormerPie()) +"," +  EBA2String.get(iep.getLatterPie())+")";  // 关系列名
        String triggerName =EBA2String.get(iep.getFormerPie())+"-"+ EBA2String.get(iep.getLatterPie())+"_triggerTime";



        this.timeData = new HashMap<>();
        this.timeData.put(formerPieSTKey, iep.getFormerStartTime() );
        this.timeData.put(latterPieSTKey, iep.getLatterStartTime() );
//        this.data.put(formerPieETKey, iep.getFormerEndTime() );
//        this.data.put(latterPieETKey, iep.getLatterEndTime() );
//        this.data.put(relationKey, iep.getRelation().toString());
        this.timeData.put(triggerName, iep.getSystemTriggerTime() );
        this.iepSource=new HashSet<IEP>();
        this.iepSource.add(iep);
        this.triggerTime=iep.getTriggerTime();
        this.indexKey=generateKey(joinColumns);
    }

    public Set<IEP> getIepSource(){
        return iepSource;
    }

    public long getTriggerTime() {
        return triggerTime;
    }

    public void update(String replaceColName,Long EndTime){
        this.timeData.put(replaceColName,EndTime);
    }

    public String getIndexKey(){
        return indexKey;
    }
    // 返回该行的所有列名
    public Set<String> getColumnNames() {
        return timeData.keySet();
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
            keyBuilder.append(timeData.get (column+".ST" )).append("|");
        }
        return keyBuilder.toString();
    }



    // 自然连接，将当前行与另一行 Row 进行连接
    public Row join(Row other, List<String> parentJoinColumns,boolean needNewIndex) {
        long lastTriggerTime=this.triggerTime>=other.triggerTime?this.triggerTime:other.triggerTime;
        Map<String, Long> joinedData = new HashMap<>(this.timeData);
        joinedData.putAll(other.timeData);
        Set<IEP> mergedIEPSource= new HashSet<>(this.getIepSource());
        mergedIEPSource.addAll(other.getIepSource());
        Row r= new Row(joinedData,parentJoinColumns,mergedIEPSource,lastTriggerTime,needNewIndex);
        return r;
    }


    public Map<String, Long> getTimeData() {
        return timeData;
    }

    public Long getValueFromColName(String colName) {
        return timeData.get(colName);
    }

    // 打印行的内容
    @Override
    public String toString() {
        return timeData.toString();
    }

    @Override
    public  boolean isExpired(long deadLine){
        return triggerTime<deadLine;
    }

    @Override
    public long getSortKey() {
        return triggerTime;
    }

    public void addCol(String colName,long value){
        this.timeData.put(colName,value);

    }

    public long getProcessTime(long value){
        long maxTriggerTime=0;
        for( Map.Entry<String,Long > entry:  this.timeData.entrySet()){
            if(entry.getKey().endsWith("_triggerTime") && entry.getValue()>maxTriggerTime ){
                maxTriggerTime =entry.getValue();
            }
        }
        return value - maxTriggerTime;
    }

}
