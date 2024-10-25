package org.example.merger;

import com.fasterxml.jackson.databind.util.LinkedNode;
import org.example.engine.MPIEPair;
import org.example.events.PointEvent;
import org.example.parser.MPIEPairSource;
import org.example.piepair.IE;
import org.example.piepair.IEP;
import org.example.piepair.TemporalRelations;
import org.example.piepair.eba.EBA;
import sun.awt.image.ImageWatched;

import java.util.*;

public class TreeNode {
    final Set<EBA> predSet;      // 与节点关联的谓词集合
    final Set<EBA> keyPredSet;   // 节点之间共享的关键谓词集合
    TreeNode parent;             // 树中的父节点
    TreeNode brother;            // 在合并过程中使用的兄弟节点
    TreeNode left;               // 树中的左子节点
    TreeNode right;              // 树中的右子节点
    final MPIEPairSource source; // 节点创建时的源对象
    int height;                  // 节点在树中的高度
    final IEPCol Col;            // 正常的指定的精确关系的检测结果队列
    Table T;                     // 表格，用于存储节点信息
    Table newT;
//    Table leafNewT;
    final IEPCol befCol;         // before关系的检测结果队列
    final IEPCol aftCol;         // after关系的检测结果队列
    final LinkList<IE> formerIEList;    // 辅助检测before关系的区间事件队列
    final LinkList<IE> latterIEList;    // 辅助检测after关系的区间事件队列
    final boolean isLeaf;        // 是否为叶子节点
    final boolean hasBefore;     // 是否包含before关系
    final boolean hasAfter;      // 是否包含after关系
//    final List<String> joinedCols;
    MPIEPair mpp;      // 是否包含after关系

    // 构造函数
    public TreeNode(Set<EBA> predSet, Set<EBA> keyPredSet, TreeNode left, TreeNode right,
                    TreeNode parent, TreeNode brother, MPIEPairSource source, int height,
                    boolean isLeaf, long QCapacity,Map<EBA, String> EBA2String) {
        this.predSet = predSet;
        this.keyPredSet = keyPredSet;
        this.left = left;
        this.right = right;
        this.parent = parent;
        this.brother = brother;
        this.source = source;
        this.height = height;
        this.isLeaf = isLeaf;
//        this.joinedCols=new ArrayList<>();
        long bef_aftQCapacity = ((QCapacity + 3) * QCapacity) / 6;


        if (isLeaf) {
            this.Col = new IEPCol(QCapacity, EBA2String);
            this.T = null;
            this.newT = null;

//            if (source.isHasAfterRel() || source.isHasBeforeRel() ){
//                this.leafNewT=new Table(bef_aftQCapacity);
//            }else {
//                this.leafNewT=new Table(QCapacity);
//            }


            if (source.isHasAfterRel()) {
                this.hasAfter = true;
                this.latterIEList = new LinkList<>(QCapacity);
                this.aftCol = new IEPCol(bef_aftQCapacity,  EBA2String);
            } else {
                this.hasAfter = false;
                this.latterIEList = null;
                this.aftCol = null;
            }

            if (source.isHasBeforeRel()) {
                this.hasBefore = true;
                this.formerIEList = new LinkList<>(QCapacity);
                this.befCol = new IEPCol(bef_aftQCapacity,EBA2String);
            } else {
                this.hasBefore = false;
                this.formerIEList = null;
                this.befCol = null;
            }
        } else {

            this.T = new Table(bef_aftQCapacity);
            this.newT = new Table(bef_aftQCapacity);
            this.Col = null;
            this.hasAfter = false;
            this.hasBefore = false;
            this.formerIEList = null;
            this.latterIEList = null;
            this.befCol = null;
            this.aftCol = null;
        }
    }

    // 叶子节点的构造函数
    public TreeNode(Set<EBA> predSet, MPIEPairSource source, long QCapacity,Map<EBA, String> EBA2String) {
        this(predSet, new HashSet<>(), null, null, null, null, source, 0, true, QCapacity,  EBA2String);
    }

    // 父节点的构造函数
    public TreeNode(Set<EBA> predSet, Set<EBA> keyPredSet, TreeNode left, TreeNode right, int height, long QCapacity,Map<EBA, String> EBA2String) {
        this(predSet, keyPredSet, left, right, null, null, null, height, false, QCapacity, EBA2String);
    }

    // Getter 和 Setter 方法


//    public List<String> getJoinedCols() {
//        return joinedCols;
//    }

//    public void setJoinedCols(List<String> joinedCols ){
//        this.joinedCols.addAll(joinedCols);
//        this.joinedCols.sort(null);
//    }

    public Set<EBA> getPredSet() {
        return predSet;
    }

    public void setMPIEPair(MPIEPair mpp) {
        this.mpp=mpp;
    }

    public Set<EBA> getKeyPredSet() {
        return keyPredSet;
    }

    public TreeNode getParent() {
        return parent;
    }

    public void setParent(TreeNode parent) {
        this.parent = parent;
    }

    public TreeNode getBrother() {
        return brother;
    }

    public void setBrother(TreeNode brother) {
        this.brother = brother;
    }

    public TreeNode getLeft() {
        return left;
    }

    public void setLeft(TreeNode left) {
        this.left = left;
    }

    public TreeNode getRight() {
        return right;
    }

    public void setRight(TreeNode right) {
        this.right = right;
    }

    public MPIEPairSource getSource() {
        return source;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public IEPCol getCol() {
        return this.Col;
    }

    public Table getT () {
        return T;
    }

    public Table getNewT() {
        return newT;
    }

    public void setTable(Table T) {
        this.T = T;
    }

    public IEPCol getBefCol() {
        return befCol;
    }

    public IEPCol getAftCol() {
        return aftCol;
    }

    public LinkList<IE> getFormerIEList() {
        return formerIEList;
    }

    public LinkList<IE> getLatterIEList() {
        return latterIEList;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public boolean isHasAfter() {
        return hasAfter;
    }

    public boolean isHasBefore() {
        return hasBefore;
    }

    // 其他方法

    public void deriveBeforeAfterRel() {
        if (hasBefore) {
            deriveBefore();
        }
        if (hasAfter) {
            deriveAfter();
        }
    }



    private void derivePreviousBefIEP( PointEvent latterPieStart){

        if(befCol.getSize()!=0){
            for ( Map.Entry<Long , List<IEP>> entry : befCol.getLong2IEPListMap(mpp.getFormerPred()).entrySet() ){
//            entry.getKey();
                IEP iep =entry.getValue().get(0);

                befCol.setTriggerMSG( new IEP(TemporalRelations.AllenRel.BEFORE,mpp.getFormerPred(),mpp.getLatterPred(),
                        iep.getFormerPieStart(),latterPieStart,iep.getFormerPieEnd(),null,
                        iep.getFormerStartTime(),latterPieStart.getTimestamp(),latterPieStart,latterPieStart.getTimestamp() )   );
            }
        }

    }
    private void deriveNewBefIEP(PointEvent latterPieStart){

        LinkList<IE>.Node  tail = formerIEList.getTail();
        if(tail!=null   ){
            IE lastFormerIE =  tail.getData();
            if (lastFormerIE.getEndTime() < latterPieStart.getTimestamp()){ // 若小于则可以 并入bef
                befCol.setTriggerMSG( new IEP(TemporalRelations.AllenRel.BEFORE,mpp.getFormerPred(),mpp.getLatterPred(),
                        lastFormerIE.getStartEvent(),latterPieStart,lastFormerIE.getEndEvent(),null,
                        lastFormerIE.getStartTime(),latterPieStart.getTimestamp(),latterPieStart,latterPieStart.getTimestamp()));
                formerIEList.deleteNode(tail);
                tail = formerIEList.getTail();
            }else{  // 这一种情况是 meets ,  则跳过最后一个元素把之前的 formerIEList 全部并入 befCol
                tail=tail.prev;
            }

            //无论如何，把 formerIEList 除了最后一个以外的都加入到 befCol；
            while(tail!=null){
                lastFormerIE =  tail.getData();
                befCol.setTriggerMSG( new IEP(TemporalRelations.AllenRel.BEFORE,mpp.getFormerPred(),mpp.getLatterPred(),
                        lastFormerIE.getStartEvent(),latterPieStart,lastFormerIE.getEndEvent(),null,
                        lastFormerIE.getStartTime(),latterPieStart.getTimestamp(),latterPieStart,latterPieStart.getTimestamp() )   );
                LinkList.Node newTail= tail.prev;
                formerIEList.deleteNode(tail);
                tail = newTail;
            }

        }
    }

    public void deriveBefore() {
        if(mpp.isHasNewLatterIE() ){
//            IE newLatIE= new IE(mpp.getLatterPred(),mpp.getLatterPieStart());
            PointEvent latterPieStart=mpp.getLatterPieStart();
            derivePreviousBefIEP(latterPieStart);
            deriveNewBefIEP(latterPieStart);
        }
    }

    public void deriveAfter() {
        if(mpp.isHasNewFormerIE() ){
//            IE newLatIE= new IE(mpp.getLatterPred(),mpp.getLatterPieStart());
            PointEvent formerPieStart=mpp.getFormerPieStart();
            derivePreviousAftIEP(formerPieStart);
            deriveNewAftIEP(formerPieStart);
        }

    }
    private void derivePreviousAftIEP(PointEvent formerPieStart){
        if(aftCol.getSize()!=0){
            for ( Map.Entry<Long , List<IEP>> entry : aftCol.getLong2IEPListMap(mpp.getLatterPred()).entrySet() ){
//            entry.getKey();
                IEP iep =entry.getValue().get(0);

                aftCol.setTriggerMSG( new IEP(TemporalRelations.AllenRel.AFTER,mpp.getFormerPred(),mpp.getLatterPred(),
                        formerPieStart,iep.getLatterPieStart(),null,iep.getLatterPieEnd(),
                        formerPieStart.getTimestamp(),iep.getLatterStartTime(),formerPieStart,formerPieStart.getTimestamp() )   );
            }
        }
    }
    private void deriveNewAftIEP(PointEvent formerPieStart){

        LinkList<IE>.Node  tail = latterIEList.getTail();
        if(tail!=null   ){
            IE lastLatterIE =  tail.getData();
            if (lastLatterIE.getEndTime() < formerPieStart.getTimestamp()){ // 若小于则可以 并入 aftCol
                aftCol.setTriggerMSG( new IEP(TemporalRelations.AllenRel.AFTER,mpp.getFormerPred(),mpp.getLatterPred(),
                        formerPieStart,lastLatterIE.getStartEvent(),null,lastLatterIE.getEndEvent(),
                        formerPieStart.getTimestamp(),lastLatterIE.getStartTime(),formerPieStart,formerPieStart.getTimestamp()));
                latterIEList.deleteNode(tail);
                tail = latterIEList.getTail();
            }else{  // 这一种情况是 meets , 则跳过最后一个元素把之前的 latterIEList 全部并入 befCol
                tail=tail.prev;
            }

            //无论如何，把 formerIEList 除了最后一个以外的都加入到 befCol；
            while(tail!=null){
                lastLatterIE =  tail.getData();
                aftCol.setTriggerMSG( new IEP(TemporalRelations.AllenRel.AFTER,mpp.getFormerPred(),mpp.getLatterPred(),
                        formerPieStart,lastLatterIE.getStartEvent(),null,lastLatterIE.getEndEvent(),
                        formerPieStart.getTimestamp(),lastLatterIE.getStartTime(),formerPieStart,formerPieStart.getTimestamp()));
                LinkList.Node newTail= tail.prev;
                latterIEList.deleteNode(tail);
                tail = newTail;

            }
        }
    }


}