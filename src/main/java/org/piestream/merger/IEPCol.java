package org.piestream.merger;

import java.util.*;

import org.piestream.engine.Window;
import org.piestream.engine.WindowType;
import org.piestream.events.PointEvent;
import org.piestream.piepair.IEP;
import org.piestream.piepair.eba.EBA;

public class IEPCol   {
    public final Map<EBA, Map<Long, List<IEP>>> colMap; // 建立在 IEPList 上的索引
    private final Table iepTable; // iepList对应的Table
    private boolean isTrigger;
    private final LinkList<IEP> newIEPList;
    private final Map<EBA, Map<Long, List<IEP>>> newIEPMap;
    private final Table newIEPTable; // newIEPList对应的Table
    private final Window window;
    private final LinkList<IEP> iepList;   // 包含 IEP 的链表
    // 构造函数，初始化 LinkList 和索引
    public IEPCol(Window window, Map<EBA, String> EBA2String ) {
        this.window=window;
        this.colMap = new HashMap<>();
        this.isTrigger = false;
        this.newIEPMap = new HashMap<>();
        this.newIEPTable=new Table(window);
        this.iepList = new LinkList<>(window);
        this.iepTable=new Table(window);
        this.newIEPList = new LinkList<>(window);
    }


    public final LinkList<IEP> getIepList(){
        return iepList;
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
        }
    }

    public void setTriggerMSG(  IEP iep){
        this.newIEPList.safeAdd(iep);
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



    // 添加 IEP 到链表和索引中，支持两个 EBA 和两个 startTime
    public void updateIEP2List() {
        if(isTrigger){
            long excess=needMoreSpaceWhenConcate(newIEPList);
            if(excess>0){
                deleteExcessiveIEPAndIndex(excess);
            }
            iepList.concat(newIEPList);  // 添加到链表
            MapMerger.mergeNestedMaps(colMap,newIEPMap );
            this.iepTable.concatenate(this.newIEPTable);
        }
    }

    private long needMoreSpaceWhenConcate(LinkList<IEP> newIepList){
        if( this.window.getWindowType()== WindowType.CAPACITY_WINDOW){
            return this.iepList.getSize()+newIepList.getSize()-this.iepList.getCapacity();
        }else{
            return 0;
        }
    }

    private void deleteExcessiveIEPAndIndex(long excessNum){
        List<IEP> toDelIepList=this.iepList.deleteFromHead(excessNum);
        for(IEP iep:toDelIepList){
            deleteHashindexByIEP(iep);
        }
    }

    // 根据 EBA 和 startTime 获取 IEP 列表
    public List<IEP> getIEP(EBA eba, Long startTime) {
        if (colMap.containsKey(eba) && colMap.get(eba).containsKey(startTime)) {
            return colMap.get(eba).get(startTime);
        }
        return new ArrayList<>();
    }

    // 根据 EBA 和  Long ->get IEP Map
    public  Map< Long , List<IEP>> getLong2IEPListMap(EBA eba ) {
        if (colMap.containsKey(eba)  ) {
            return colMap.get(eba);
        }
        return   null;
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
    public long getSize() {
        return iepList.getSize();
    }

    // 获取链表的容量
    public long getCapacity() {
        return window.getWindowCapacity();
    }

    // 获取头 IEP
    public IEP getHeadIEP() {
        return iepList.getHead().data;
    }

    // 获取尾 IEP
    public IEP getTailIEP() {
        return iepList.getTail().data;
    }

    //refresh 时删除超过 window的ColMap
    private void deleteHashindexByIEP(IEP iep){
        // 更新 hashIndex 中的索引
        EBA formerPie = iep.getFormerPie();
        long formerStart=iep.getFormerStartTime();
        Map<Long, List<IEP>> formerMap= colMap.get(formerPie);
        List<IEP> sameIndexIEPList = formerMap.get(formerStart);
        if (sameIndexIEPList != null) {
            sameIndexIEPList.remove(iep);
            if (sameIndexIEPList.isEmpty()) {
                formerMap.remove(formerStart);
            }
        }


        EBA latterPie = iep.getLatterPie();
        long latterStart=iep.getLatterStartTime();
        Map<Long, List<IEP>> latterMap= colMap.get(latterPie);
        sameIndexIEPList = latterMap.get(latterStart);
        if (sameIndexIEPList != null) {
            sameIndexIEPList.remove(iep);
            if (sameIndexIEPList.isEmpty()) {
                latterMap.remove(latterStart);
            }
        }
    }

    public List<IEP>  refresh(long deadLine ){
        List<IEP> toDelIeps=iepList.refresh(deadLine);
        // del ColMap which relate to toDelIeps
         for(IEP iep:toDelIeps){
            deleteHashindexByIEP(iep);
        }
        // refresh iepTable
        if (iepTable.getSize() != 0) {
            iepTable.refresh(deadLine );
        }
        return toDelIeps;

    }


    // 把叶子结点中的本次发现的bef结果 更新到大表中，方便后续进行统一Join
    public void mergeBefAftCol(IEPCol col){
        this.getNewIEPTable().concatenate(col.getNewIEPTable());
        this.isTrigger=true;
    }


}
