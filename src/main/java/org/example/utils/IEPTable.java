//package org.example.utils;
//import java.util.*;
//
//import org.example.piepair.IEP;
//import org.example.piepair.dfa.Node;
//import org.example.piepair.eba.EBA;
//
//public class IEPTable {
//
//    private Set<EBA> keyPredSet;                           // 关键谓词集合
//    private TreeNode node;                                 // 树节点
//    private int rowCapacity;                               // 表格的最大行数
//    private int colNum;                                    // 每行的列数（IEP数）
//    private LinkList<IEPRow> table;                            // 表格，存储表格的所有行
//    private Map<EBA, Map<Long, List<IEPRow> >> indexMap;    // 哈希索引，映射到多个 IEPRow
//    private boolean isUpdated;
//    private  LinkList<IEPRow> updateTable;
//    private Map<EBA, Map<Long, List<IEPRow> >> updateindexMap;
//
//
//    // 构造函数，初始化表格和索引
//    public IEPTable(Set<EBA> keyPredSet, int colNum, int rowCapacity) {
//        this.keyPredSet = keyPredSet;
//        this.colNum = colNum;
//        this.rowCapacity = rowCapacity;
//        this.table = new LinkList<IEPRow>(rowCapacity);
//        this.indexMap = new HashMap<>();
//        this.isUpdated=false;
//        this.updateTable=new LinkList<IEPRow>(rowCapacity);
//        this.updateindexMap = new HashMap<>();
//        // 初始化索引，针对 keyPredSet 中的每个 EBA 进行索引初始化
//        for (EBA pred : keyPredSet) {
//            indexMap.put(pred, new HashMap<>());
//        }
//    }
//
//
//    public boolean getIsUpdated(){
//        return isUpdated;
//    }
//
//    public LinkList<IEPRow> getUpdateTable(){
//        return updateTable;
//    }
//
//    public Map<EBA, Map<Long, List<IEPRow> >> getUpdateindexMap(){
//        return updateindexMap;
//    }
//    private void updateMSG(){
//        table.concat(updateTable);
//        MapMerger.mergeNestedMaps(indexMap,updateindexMap);
//        resetIsUpdated();
//    }
//
//
//    public void resetIsUpdated(){
//        isUpdated =false;
//        updateTable.clear();
//        updateindexMap.clear();
//    }
//
//    public void setUpdateMSG(EBA pred1,Long startTime1,EBA pred2,Long startTime2,IEPRow newRow){
//        updateTable.add(newRow);
//        addMap(updateindexMap,  pred1,  startTime1,    pred2,  startTime2,  newRow );
//    }
//
//    public void setUpdateMSG(EBA pred1,Long startTime1 ,IEPRow newRow){
//        updateTable.add(newRow);
//        addMap(updateindexMap,  pred1,  startTime1, newRow );
//    }
//
//
//    // 构建行，将 iep 与 row 构成一个新的 row，并返回
//    public IEPRow buildRow(IEPRow row, IEP iep) {
//        if (row.isFull()) {
//            System.out.println("Row is full, cannot add more IEPs.");
//            return null;
//        }
//        IEPRow newRow = new IEPRow(colNum);
//        newRow.getIEPList().addAll(row.getIEPList());
//        newRow.addIEP(iep);
//        return newRow;
//    }
//
//    public List<IEPRow>  buildRow(List<IEPRow> rows, IEP iep) {
//        List<IEPRow> newRows=new ArrayList<>();
//        for (IEPRow r :rows ){
//            newRows.add(buildRow(r,iep));
//        }
//        return newRows;
//    }
//
//
//    // 构建行，将 iep 构成一个新的 row，并返回
//    public IEPRow buildRow(IEP iep) {
//        IEPRow newRow = new IEPRow(colNum);
//        newRow.addIEP(iep);
//        return newRow;
//    }
//
//    public boolean mergeOneCol(IEP iep ) { //最下层的结点
//        if (keyPredSet.size() != 2) {
//            throw new IllegalArgumentException("KeyPredSet size must be 2 for this operation.");
//        }
//        EBA pred1 = keyPredSet.iterator().next();
//        EBA pred2 = keyPredSet.iterator().next();
//        IEPRow newRow = buildRow(iep);  // 构建新行
//        Long startTime1 = iep.getStartTime(pred1);
//        Long startTime2 = iep.getStartTime(pred2);
//        setUpdateMSG(pred1,startTime1,pred2,startTime2,newRow);
//        return true;
//    }
//
//    public void  mergeTableWithCol(IEPTable tb, IEPCol col ) { //最下层的结点
//        if (tb.getIsUpdated() && ! col.getIsTrigger() ){
//            mergeNewTableWithOldCol(  tb.getUpdateTable(),  col );
//        }
//        else if(! tb.getIsUpdated() &&   col.getIsTrigger()){
//            mergeOldTableWithNewIEP(tb,col);
//        }
//        else {
//            mergeNewTableWithOldCol(tb.getUpdateTable(),col);
//            mergeOldTableWithNewIEP(tb,col);
//            mergeNewTableWithNewIEP(tb.getUpdateTable(),col );
//
//        }
//
//    }
//
//    private void mergeOldTableWithNewIEP(IEPTable tb,IEPCol col ){
//
////        List<IEPRow> Rows=new ArrayList<IEPRow>();
//        Iterator<EBA> iterator = keyPredSet.iterator();
//        EBA pred1 = iterator.next();
//        Map<EBA, Map<Long, List<IEP>>> colTriggerMap =col.getTriggerMap();
//        Map<Long,List<IEP>> colPredIndex1 =colTriggerMap.get(pred1);
//        Long startTime1=colPredIndex1.keySet().iterator().next();
//        Map<EBA, Map<Long, List<IEPRow>>>  tbMap=tb.indexMap;
//
//        if (keyPredSet.size() == 2) {
//            EBA pred2 = iterator.next();
//            Map<Long,List<IEP>> colPredIndex2 =colTriggerMap.get(pred2);
//            Long startTime2=colPredIndex1.keySet().iterator().next();
//            List<IEPRow> foundOldRows=findIntersection(tbMap,pred1,startTime1,pred2,startTime2);
////            MapMerger.mergeNestedMaps(tbMap,colTriggerMap);
//
//            List<IEPRow> newRows=buildRow(foundOldRows,col.getTriggerIEP());
//
//
//        }
//
//
//        Map<Long,List<IEPRow>> tbPredIndex1 =tb.indexMap.get(pred1 );
//        List<IEPRow> oldRows=tbPredIndex1.get(startTime1);
//
//        //构建新的 rows
//        List<IEPRow> newRows=buildRow(oldRows,col.getTriggerIEP());
//
//
//
//    }
//
//    private void mergeNewTableWithOldCol(LinkList<IEPRow> tb, IEPCol col ){
//
//    }
//    private void mergeNewTableWithNewIEP(LinkList<IEPRow> tb,IEPCol col ){
//
//    }
//
//
//
//
//    // 更新索引函数：基于两个 EBA 和开始时间，更新单个行
//    public void addMap( Map<EBA, Map<Long, List<IEPRow> >> indMap ,EBA pred1, Long startTime1, EBA pred2, Long startTime2, IEPRow row) {
//        // 更新 pred1 的索引
//        Map<Long, List<IEPRow>> predIndex1 = indMap.computeIfAbsent(pred1, k -> new HashMap<>());
//        predIndex1.computeIfAbsent(startTime1, k -> new ArrayList<>()).add(row);
//
//        // 更新 pred2 的索引
//        Map<Long, List<IEPRow>> predIndex2 = indMap.computeIfAbsent(pred2, k -> new HashMap<>());
//        predIndex2.computeIfAbsent(startTime2, k -> new ArrayList<>()).add(row);
//    }
//
//    public void addMap( Map<EBA, Map<Long, List<IEPRow> >> indMap ,EBA pred1, Long startTime1,  IEPRow row) {
//        // 更新 pred1 的索引
//        Map<Long, List<IEPRow>> predIndex1 = indMap.computeIfAbsent(pred1, k -> new HashMap<>());
//        predIndex1.computeIfAbsent(startTime1, k -> new ArrayList<>()).add(row);
//
//    }
//
//
//    public List<IEPRow> mergeRowWithTableUpdate(IEPTable tb, IEPCol Col){
//        List<IEPRow> rows =new ArrayList<IEPRow>();
//
//
//        return rows ;
//    }
//
//    public void joinFully  (IEPTable tb, IEPCol Col){
//
//        List<IEPRow> Rows=new ArrayList<IEPRow>();
//        Iterator<EBA> iterator = keyPredSet.iterator();
//        EBA pred1 = iterator.next();
//
//        if (keyPredSet.size() == 1) {
//            Map<Long,List<IEPRow>>  Time2RowList1= tb.indexMap.get(pred1);
//            Map<Long, List<IEP>> Time2IEPList1= Col.colMap.get(pred1);
//            for(Map.Entry<Long, List<IEPRow>> entry : Time2RowList1.entrySet()) {
//                Long timestart= entry.getKey();
//                List<IEPRow> RowList = entry.getValue();
//                List<IEP> IEPList=Time2IEPList1.get(timestart);
//                Rows.addAll(buildRow(RowList,IEPList));
//            }
//        }
//
//        if (keyPredSet.size() == 2) {
//            EBA pred2 = iterator.next();
//
//            Map<Long,List<IEPRow>>  Time2RowList1= tb.indexMap.get(pred1);
//            Map<Long,List<IEPRow>>  Time2RowList2= tb.indexMap.get(pred2);
//            Map<Long, List<IEP>> Time2IEPList1= Col.colMap.get(pred1);
//            Map<Long, List<IEP>> Time2IEPList2= Col.colMap.get(pred2);
//
//            for(Map.Entry<Long, List<IEPRow>> entry1 : Time2RowList1.entrySet()) {
//                Long timestart1= entry1.getKey();
//                Set<IEPRow> RowSet =new HashSet<>( entry1.getValue());
//                Set<IEP> IEPSet=new HashSet<>(Time2IEPList1.get(timestart1));
//                for(Map.Entry<Long, List<IEPRow>> entry2 : Time2RowList2.entrySet()){
//                    Long timestart2= entry2.getKey();
//                    RowSet.retainAll( new HashSet<>( entry2.getValue()) );
//                    if (RowSet.isEmpty()){
//                        continue;
//                    }
//                    IEPSet.retainAll(new HashSet<>(Time2IEPList2.get(timestart2)) );
//                    Rows.addAll(buildRow(new ArrayList<>(RowSet),new ArrayList<>(IEPSet)));
//                }
//            }
//
//        }
//
////        addAll2UpdatedIEPRow(Rows);
//    }
//
//    public List<IEPRow> buildRow(List<IEPRow> RowList,List<IEP> IEPList){
//        List<IEPRow> Rows=new ArrayList<IEPRow>();
//        for(IEPRow r:RowList){
//            for (IEP iep:IEPList){
//                Rows.add(buildRow(r,iep));
//            }
//        }
//        return Rows;
//    }
//
//
////
////    public boolean mergeRow  (IEPTable tb, IEPCol Col){
////        List<IEPRow> rows =new ArrayList<IEPRow>();
////
////        if ((tb.getIsUpdated() == false) &&  Col.getIsTrigger() ){
////            rows.addAll(mergeTable_IEP(tb,Col.getTriggerIEP()));
////
////        }else if(tb.getIsUpdated()  &&  (Col.getIsTrigger() == false) ){
////            rows.addAll(mergeRowWithTableUpdate(tb,Col));
////        }
////        else { //都更新了
////            rows.addAll(mergeRowWithColUpdate(tb,Col));
////            rows.addAll(mergeRowWithTableUpdate(tb,Col));
////        }
////        addAll2UpdatedIEPRow(rows);
////
////    }
//
//    public List<IEPRow> mergeTable_IEP(IEPTable t, IEP iep) {
//        if (keyPredSet.size() == 1) {
//            EBA pred = keyPredSet.iterator().next();
//            Long startTime = iep.getStartTime(pred);
//
//            // 在 t 的索引中查找对应的 rows
//            Map<Long, List<IEPRow>> predIndex = t.indexMap.get(pred);
//            List<IEPRow> foundRows = new ArrayList<>();
//            if (predIndex.containsKey(startTime)) {
//                List<IEPRow> rows = predIndex.get(startTime);
//                for (IEPRow row : rows) {
//                    IEPRow newRow = buildRow(row, iep); // 构建新行
//                    if (newRow != null) {
//                        foundRows.add(newRow);  // 添加找到的行
//                    }
//                }
//            }
//
//            // 如果没有找到可以合并的行，则返回 false
//            if (foundRows.isEmpty()) {
//                return null;
//            }
//
//
//            return foundRows;
//        } else if (keyPredSet.size() == 2) {
//            // 对两个 EBA 的开始时间进行交集查找
//            Iterator<EBA> iterator = keyPredSet.iterator();
//            EBA pred1 = iterator.next();
//            EBA pred2 = iterator.next();
//
//            Long startTime1 = iep.getStartTime(pred1);
//            Long startTime2 = iep.getStartTime(pred2);
//
//            Map<Long, List<IEPRow>> predIndex1 = t.indexMap.get(pred1);
//            Map<Long, List<IEPRow>> predIndex2 = t.indexMap.get(pred2);
//
//            List<IEPRow> foundRows = new ArrayList<>();
//            // 确保两个开始时间都存在
//
//            if (predIndex1.containsKey(startTime1) && predIndex2.containsKey(startTime2)) {
//                // 将 rows1 和 rows2 转换为 Set
//                Set<IEPRow> set1 = new HashSet<>(predIndex1.get(startTime1));
//                Set<IEPRow> set2 = new HashSet<>(predIndex2.get(startTime2));
//
//                // 取交集
//                set1.retainAll(set2);
//
//                // 处理交集的行
//                if (!set1.isEmpty()) {
//                    for (IEPRow row : set1) {
//                        IEPRow newRow = buildRow(row, iep); // 构建新行
//                        if (newRow != null) {
//                            foundRows.add(newRow);
//                        }
//                    }
//                    return foundRows;
//                }
//            }
//
//            return null; // 如果没有找到交集或无法合并
//        } else {
//            throw new IllegalArgumentException("KeyPredSet size must be 1 or 2 for this operation.");
//        }
//    }
//
//
////    public boolean addRow(IEPRow row) {
////        if (table.getSize() >= rowCapacity) {
////            IEPRow delRow=table.getHead().data;
////            removeRowIndex(delRow);  // 删除该行的索引
////            table.deleteHead();  // 从表中删除尾部行
////        }
////
////        add2UpdatedIEPRow(row);
////
////        table.add(row);  // 仅添加行，不处理索引更新
////        return true;  // 添加成功
////    }
//
//    // 删除行的索引
//    private void removeRowIndex(IEPRow row) {
//        // 遍历 keyPredSet，获取每个 EBA 的开始时间，并从 indexMap 中删除对应行
//        for (EBA pred : keyPredSet) {
//            Long startTime = row.getIEPList().get(0).getStartTime(pred);  // 获取行中第一个 IEP 的开始时间
//            Map<Long, List<IEPRow>> predIndex = indexMap.get(pred);
//
//            if (predIndex != null && predIndex.containsKey(startTime)) {
//                List<IEPRow> rows = predIndex.get(startTime);
//                rows.remove(row);  // 从列表中移除行
//
//                // 如果列表为空，将该开始时间从索引中移除
//                if (rows.isEmpty()) {
//                    predIndex.remove(startTime);
//                }
//            }
//        }
//    }
//
////
////    // 更新索引函数：基于单个 EBA 和开始时间
////    public void updateIndex(EBA pred, Long startTime, IEPRow row) {
////        // 使用 computeIfAbsent 来简化 Map 初始化
////        Map<Long, List<IEPRow>> predIndex = indexMap.computeIfAbsent(pred, k -> new HashMap<>());
////        predIndex.computeIfAbsent(startTime, k -> new ArrayList<>()).add(row);
////    }
////
////    // 更新索引函数：基于单个 EBA 和开始时间，批量添加行
////    public void updateIndex(EBA pred, Long startTime, List<IEPRow> rows) {
////        // 使用 computeIfAbsent 来简化 Map 初始化
////        Map<Long, List<IEPRow>> predIndex = indexMap.computeIfAbsent(pred, k -> new HashMap<>());
////        predIndex.computeIfAbsent(startTime, k -> new ArrayList<>()).addAll(rows);
////    }
////
////    // 更新索引函数：基于两个 EBA 和开始时间，更新单个行
////    public void updateIndex(EBA pred1, Long startTime1, EBA pred2, Long startTime2, IEPRow row) {
////        // 更新 pred1 的索引
////        Map<Long, List<IEPRow>> predIndex1 = indexMap.computeIfAbsent(pred1, k -> new HashMap<>());
////        predIndex1.computeIfAbsent(startTime1, k -> new ArrayList<>()).add(row);
////
////        // 更新 pred2 的索引
////        Map<Long, List<IEPRow>> predIndex2 = indexMap.computeIfAbsent(pred2, k -> new HashMap<>());
////        predIndex2.computeIfAbsent(startTime2, k -> new ArrayList<>()).add(row);
////    }
////
////    // 更新索引函数：基于两个 EBA 和开始时间，批量添加行
////    public void updateIndex(EBA pred1, Long startTime1, EBA pred2, Long startTime2, List<IEPRow> rows) {
////        // 更新 pred1 的索引
////        Map<Long, List<IEPRow>> predIndex1 = indexMap.computeIfAbsent(pred1, k -> new HashMap<>());
////        predIndex1.computeIfAbsent(startTime1, k -> new ArrayList<>()).addAll(rows);
////
////        // 更新 pred2 的索引
////        Map<Long, List<IEPRow>> predIndex2 = indexMap.computeIfAbsent(pred2, k -> new HashMap<>());
////        predIndex2.computeIfAbsent(startTime2, k -> new ArrayList<>()).addAll(rows);
////    }
//
//
//    // 静态方法，用于找到 indexMap 中两个条件下的交集
//    public static List<IEPRow> findIntersection(Map<EBA, Map<Long, List<IEPRow>>> indexMap, EBA pred1, Long startTime1, EBA pred2, Long startTime2) {
//        // 从 indexMap 中根据 pred1 和 startTime1 获取 List<IEPRow>
//        List<IEPRow> listA = indexMap.getOrDefault(pred1, Collections.emptyMap()).getOrDefault(startTime1, Collections.emptyList());
//
//        // 从 indexMap 中根据 pred2 和 startTime2 获取 List<IEPRow>
//        List<IEPRow> listB = indexMap.getOrDefault(pred2, Collections.emptyMap()).getOrDefault(startTime2, Collections.emptyList());
//
//        // 创建 listA 的副本
//        List<IEPRow> intersectionList = new ArrayList<>(listA);
//
//        // 找到 listA 和 listB 的交集
//        intersectionList.retainAll(listB);
//
//        // 返回交集
//        return intersectionList;
//    }
//
//
//    public void printTable(){
//        table.printList();
//    }
//
//}
