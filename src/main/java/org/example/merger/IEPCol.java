package org.example.merger;

import java.util.*;

import org.example.events.PointEvent;
import org.example.piepair.IEP;
import org.example.piepair.eba.EBA;

public class IEPCol {

    private LinkList<IEP> iepList;   // 包含 IEP 的链表
    public Map<EBA, Map<Long, List<IEP>>> colMap; // 建立在 IEPList 上的索引
    private Table iepTable; // iepList对应的Table

    private boolean isTrigger;
    private LinkList<IEP> newIEPList;
    private Map<EBA, Map<Long, List<IEP>>> newIEPMap;
    private Table newIEPTable; // newIEPList对应的Table


    public LinkList<IEP> getIepList(){
        return iepList;
    }


    // 构造函数，初始化 LinkList 和索引
    public IEPCol(long capacity,Map<EBA, String> EBA2String ) {
        this.iepList = new LinkList<>(capacity);
        this.colMap = new HashMap<>();
        this.iepTable=new Table(capacity);
        this.isTrigger = false;
        this.newIEPList = new LinkList<>(capacity);
        this.newIEPMap = new HashMap<>();
        this.newIEPTable=new Table(capacity);

    }

    public LinkList<IEP> getNewIEPList(){
        return newIEPList;
    }

    public Map<EBA, Map<Long, List<IEP>>>  getNewIEPMap(){
        return newIEPMap;
    }

    public Table getNewIEPTable(){
        return newIEPTable;
    }

    public Table getIEPTable(){
        return iepTable ;
    }

    public void resetIsTrigger() {
        if(isTrigger==true){
            isTrigger = false;
            newIEPList.clear();
            newIEPMap.clear();
            newIEPTable.clear();
        }
    }

    public boolean getIsTrigger() {
        return isTrigger;
    }

    public void printNewIEPList() {
        if (newIEPList.getSize()!=0){
            System.out.println("    +"+newIEPList.getSize());

//            newIEPList.printList();
        }
    }


//    public void setTriggerMSG(EBA pred1, Long startTime1, EBA pred2, Long startTime2, IEP iep){
//
//        this.newIEPList.add(iep);
//        this.isTrigger = true;
//        Map<Long, List<IEP>> predIndex1 = newIEPMap.computeIfAbsent(pred1, k -> new HashMap<>());
//        predIndex1.computeIfAbsent(startTime1, k -> new ArrayList<>()).add(iep);
//        Map<Long, List<IEP>> predIndex2 = newIEPMap.computeIfAbsent(pred2, k -> new HashMap<>());
//        predIndex2.computeIfAbsent(startTime2, k -> new ArrayList<>()).add(iep);
//        this.newIEPTable.addRow(iep, EBA2String);
//    }

    public void setTriggerMSG(  IEP iep){
        this.newIEPList.add(iep);
        Map<Long, List<IEP>> predIndex1 = newIEPMap.computeIfAbsent(iep.getFormerPie(), k -> new HashMap<>());
        predIndex1.computeIfAbsent(iep.getFormerStartTime(), k -> new ArrayList<>()).add(iep);
        Map<Long, List<IEP>> predIndex2 = newIEPMap.computeIfAbsent(iep.getLatterPie(), k -> new HashMap<>());
        predIndex2.computeIfAbsent(iep.getLatterStartTime(), k -> new ArrayList<>()).add(iep);
        this.isTrigger = true;

    }

    public void updateNewIepList2Table( Map<EBA, String> EBA2String,List<String> joinColumns){

        LinkList<IEP>.Node current = this.newIEPList.getHead() ;
        while (current != null) {
            this.newIEPTable.addRow(current.getData(), EBA2String,joinColumns);
            current = current.next;
        }
    }

    public void updateNewIepList2Table( Map<EBA, String> EBA2String,Set<String> joinColumns){

        LinkList<IEP>.Node current = this.newIEPList.getHead() ;
        while (current != null) {
            this.newIEPTable.addRow(current.getData(), EBA2String,joinColumns);
            current = current.next;
        }
    }

    // 添加 IEP 到链表和索引中，支持两个 EBA 和两个 startTime
    public void updateIEP2List() {
        if(isTrigger){
            iepList.concat(newIEPList);  // 添加到链表
            MapMerger.mergeNestedMaps(colMap,newIEPMap );
            this.iepTable.concatenate(this.newIEPTable);
        }
    }

    // 根据 EBA 和 startTime 获取 IEP 列表
    public List<IEP> getIEP(EBA eba, Long startTime) {
        if (colMap.containsKey(eba) && colMap.get(eba).containsKey(startTime)) {
            return colMap.get(eba).get(startTime);
        }
        return new ArrayList<>();
    }

    // 根据 EBA 和  Long -> IEP Map
    public  Map< Long , List<IEP>> getLong2IEPListMap(EBA eba ) {
        if (colMap.containsKey(eba)  ) {
            return colMap.get(eba);
        }
        return   null;
    }


    // 删除 IEP，根据两个 EBA 和对应的 startTime 更新索引
    public void deleteIEP(EBA pred1, Long startTime1, EBA pred2, Long startTime2, IEP iep) {
        iepList.delete(iep);  // 从链表中删除

        // 删除在第一个 EBA 和 startTime1 上的索引
        if (colMap.containsKey(pred1)) {
            Map<Long, List<IEP>> timeMap1 = colMap.get(pred1);
            if (timeMap1.containsKey(startTime1)) {
                List<IEP> iepListAtTime1 = timeMap1.get(startTime1);
                iepListAtTime1.remove(iep);  // 从索引中删除 IEP

                // 如果该时间点下的 IEP 列表为空，则移除该时间点
                if (iepListAtTime1.isEmpty()) {
                    timeMap1.remove(startTime1);
                }

                // 如果 EBA1 对应的所有时间点都为空，则移除该 EBA1
                if (timeMap1.isEmpty()) {
                    colMap.remove(pred1);
                }
            }
        }

        // 删除在第二个 EBA 和 startTime2 上的索引
        if (colMap.containsKey(pred2)) {
            Map<Long, List<IEP>> timeMap2 = colMap.get(pred2);
            if (timeMap2.containsKey(startTime2)) {
                List<IEP> iepListAtTime2 = timeMap2.get(startTime2);
                iepListAtTime2.remove(iep);  // 从索引中删除 IEP

                // 如果该时间点下的 IEP 列表为空，则移除该时间点
                if (iepListAtTime2.isEmpty()) {
                    timeMap2.remove(startTime2);
                }

                // 如果 EBA2 对应的所有时间点都为空，则移除该 EBA2
                if (timeMap2.isEmpty()) {
                    colMap.remove(pred2);
                }
            }
        }
    }

    // 打印 colMap 的内容
    public void print() {
        for (EBA eba : colMap.keySet()) {
            System.out.println("EBA: " + eba);
            Map<Long, List<IEP>> timeMap = colMap.get(eba);
            for (Long time : timeMap.keySet()) {
                System.out.println("  Time: " + time + " -> IEPs: " + timeMap.get(time));
            }
        }
    }

    public void updateCompletedMSG(String thePred,EBA pred, Long startTime , PointEvent endEvent ){
        if(thePred=="former"){
            for ( IEP iep: this.getIEP(pred,startTime) ){
                iep.setFormerPieEnd( endEvent);
            }
        }else if(thePred=="latter"){
            for ( IEP iep: this.getIEP(pred,startTime) ){
                iep.setLatterPieEnd( endEvent);
            }
        }
    }

    // 新增：打印 iepList 链表中的所有 IEP
    public void printCol() {
        iepList.printList();
    }

    // 获取链表的大小
    public int getSize() {
        return iepList.getSize();
    }

    // 获取链表的容量
    public long getCapacity() {
        return iepList.getCapacity();
    }

    // 获取头 IEP
    public IEP getHeadIEP() {
        return iepList.getHead().data;
    }

    // 获取尾 IEP
    public IEP getTailIEP() {
        return iepList.getTail().data;
    }
}
