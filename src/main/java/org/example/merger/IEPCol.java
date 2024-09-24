package org.example.utils;

import java.util.*;

import org.example.piepair.IEP;
import org.example.piepair.eba.EBA;

public class IEPCol {

    private LinkList<IEP> iepList;   // 包含 IEP 的链表
    public Map<EBA, Map<Long, List<IEP>>> colMap; // 建立在 IEPList 上的索引
    private boolean isTrigger;
    private IEP triggerIEP;
    private Map<EBA, Map<Long, List<IEP>>> triggerMap;

//    private EBA triggerPred1;
//    private Long triggerStartTime1;
//    private EBA triggerPred2;
//    private Long triggerStartTime2;

    public LinkList<IEP> getIepList(){
        return iepList;
    }

    public IEP getTriggerIEP() {
        return triggerIEP;
    }

    public Map<EBA, Map<Long, List<IEP>>> getTriggerMap() {
        return triggerMap;
    }

    public void resetIsTrigger() {
        isTrigger = false;
        triggerMap.clear();
    }

    public boolean getIsTrigger() {
        return isTrigger;
    }

    // 构造函数，初始化 LinkList 和索引
    public IEPCol(int capacity) {
        this.iepList = new LinkList<>(capacity);
        this.colMap = new HashMap<>();
        this.isTrigger = false;
        this.triggerIEP = null;
        this.triggerMap = new HashMap<>();

    }

    public void setTriggerMSG(EBA pred1, Long startTime1, EBA pred2, Long startTime2, IEP iep){
        if (isTrigger) {
            throw new IllegalStateException("The trigger has already been set."); // 抛出异常，表示已经触发过
        }
        this.triggerIEP=iep;
        this.isTrigger = true;
        Map<Long, List<IEP>> predIndex1 = triggerMap.computeIfAbsent(pred1, k -> new HashMap<>());
        predIndex1.computeIfAbsent(startTime1, k -> new ArrayList<>()).add(iep);
        Map<Long, List<IEP>> predIndex2 = triggerMap.computeIfAbsent(pred2, k -> new HashMap<>());
        predIndex2.computeIfAbsent(startTime2, k -> new ArrayList<>()).add(iep);

    }

    // 添加 IEP 到链表和索引中，支持两个 EBA 和两个 startTime
    public void updateIEP2List() {
        if(isTrigger){
            iepList.add(triggerIEP);  // 添加到链表
            MapMerger.mergeNestedMaps(colMap,triggerMap );
        }
    }

    // 根据 EBA 和 startTime 获取 IEP 列表
    public List<IEP> getIEP(EBA eba, Long startTime) {
        if (colMap.containsKey(eba) && colMap.get(eba).containsKey(startTime)) {
            return colMap.get(eba).get(startTime);
        }
        return new ArrayList<>();
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

    // 新增：打印 iepList 链表中的所有 IEP
    public void printCol() {
        iepList.printList();
    }

    // 获取链表的大小
    public int getSize() {
        return iepList.getSize();
    }

    // 获取链表的容量
    public int getCapacity() {
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
