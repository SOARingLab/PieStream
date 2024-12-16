package org.piestream.merger;

import org.piestream.engine.MPIEPair;
import org.piestream.engine.Window;
import org.piestream.events.PointEvent;
import org.piestream.parser.MPIEPairSource;
import org.piestream.piepair.IE;
import org.piestream.piepair.IEP;
import org.piestream.piepair.TemporalRelations;
import org.piestream.piepair.eba.EBA;

import java.util.*;

public class TreeNode {
    final Set<EBA> predSet;      // 与节点关联的谓词集合
    final Set<EBA> keyPredSet;   // 节点之间共享的关键谓词集合 join key
    TreeNode parent;             // 树中的父节点
    TreeNode brother;            // 在合并过程中使用的兄弟节点
    TreeNode left;               // 树中的左子节点
    TreeNode right;              // 树中的右子节点
    final MPIEPairSource source; // 节点创建时的源对象
    int height;                  // 节点在树中的高度
    final IEPCol Col;            // 正常的指定的精确关系的检测结果队列
    Table T;                     // 表格，用于存储节点信息
    Table newT;
    final IEPCol befCol;         // before关系的检测结果队列
    final IEPCol aftCol;         // after关系的检测结果队列
    final LinkList<IE> formerIEList;    // 辅助检测before关系的区间事件队列
    final LinkList<IE> latterIEList;    // 辅助检测after关系的区间事件队列
    final boolean isLeaf;        // 是否为叶子节点
    final boolean hasBefore;     // 是否包含before关系
    final boolean hasAfter;      // 是否包含after关系
    final Window window;
    MPIEPair mpp;      // 是否包含after关系

    long resCount;
    long processTime;
    // 构造函数
    public TreeNode(Set<EBA> predSet, Set<EBA> keyPredSet, TreeNode left, TreeNode right,
                    TreeNode parent, TreeNode brother, MPIEPairSource source, int height,
                    boolean isLeaf, Window window, Map<EBA, String> EBA2String) {
        this.window=window;
        this.predSet = predSet;
        this.keyPredSet = keyPredSet;
        this.left = left;
        this.right = right;
        this.parent = parent;
        this.brother = brother;
        this.source = source;
        this.height = height;
        this.isLeaf = isLeaf;
        this.resCount=0;
        this.processTime=0;
//        this.joinedCols=new ArrayList<>();
        Window bef_aftWindow = window;


        if (isLeaf) {
            this.Col = new IEPCol(window,EBA2String);
            this.T = null;
            this.newT = null;

            if (source.isHasAfterRel()) {
                this.hasAfter = true;
                this.latterIEList = new LinkList<>(window);
                this.aftCol = new IEPCol(bef_aftWindow,  EBA2String);
            } else {
                this.hasAfter = false;
                this.latterIEList = null;
                this.aftCol = null;
            }

            if (source.isHasBeforeRel()) {
                this.hasBefore = true;
                this.formerIEList = new LinkList<>(window);
                this.befCol = new IEPCol(bef_aftWindow,EBA2String);
            } else {
                this.hasBefore = false;
                this.formerIEList = null;
                this.befCol = null;
            }
        } else {

            this.T = new Table(bef_aftWindow);
            this.newT = new Table(bef_aftWindow);
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
    public TreeNode(Set<EBA> predSet, MPIEPairSource source,   Window window,Map<EBA, String> EBA2String) {
        this(predSet, new HashSet<>(), null, null, null, null, source, 0, true,window,  EBA2String);
    }

    // 父节点的构造函数
    public TreeNode(Set<EBA> predSet, Set<EBA> keyPredSet, TreeNode left, TreeNode right, int height,  Window window,Map<EBA, String> EBA2String) {
        this(predSet, keyPredSet, left, right, null, null, null, height, false,window, EBA2String);
    }


    public Set<EBA> getPredSet() {
        return predSet;
    }

    public long getResCount() {
        return resCount;
    }

    public void addResCount(long n) {
        this.resCount += n;
    }

    public void addProcessTime(long n) {
        this.processTime += n;
    }

    public long getProcessTime(long n) {
        return processTime;
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

    // TODO: needDerivePrev=true;
    public void deriveBeforeAfterRel(boolean needDerivePrev) {
//        needDerivePrev=true;
        if (hasBefore) {
            if(mpp.isHasNewLatterIE() ){
                PointEvent latterPieStart=mpp.getLatterPieStart();
                LinkList.Node tail=deriveClosestBefIEP(latterPieStart);
                if(needDerivePrev){
                    derivePreviousBefFromIE(tail,latterPieStart);
                }
            }
        }
        if (hasAfter) {
            if(mpp.isHasNewFormerIE() ){
                PointEvent formerPieStart=mpp.getFormerPieStart();
                LinkList.Node tail=deriveClosestAftIEP(formerPieStart);
                if(needDerivePrev){
                    derivePreviousAftFromIE(tail,formerPieStart);
                }

            }
        }
    }

    private LinkList<IE>.Node  deriveClosestBefIEP(PointEvent latterPieStart){

        LinkList<IE>.Node  tail = formerIEList.getTail();
        if(tail!=null   ){
            IE lastFormerIE =  tail.getData();
            // find the last FormerIE that choud just satisfy before();
            // we can sure that : lastFormerIE.getStartTime()<=latterPieStart.getTimestamp()
            if(lastFormerIE.getStartTime()==latterPieStart.getTimestamp() ){ // starts , started-by
                tail=tail.prev;
            }
            else if(lastFormerIE.getEndTime()==null ){  // lastFormerIE is not compeleted
                tail=tail.prev;
            }
            else if(lastFormerIE.getEndTime() == latterPieStart.getTimestamp()) {  //  meets
                tail=tail.prev;
            }
            else { // followed-by 理想情况

            }
            lastFormerIE =  tail.getData();
            befCol.setTriggerMSG( new IEP(TemporalRelations.AllenRel.BEFORE,mpp.getFormerPred(),mpp.getLatterPred(),
                    lastFormerIE.getStartEvent(),latterPieStart,lastFormerIE.getEndEvent(),null,
                    lastFormerIE.getStartTime(),latterPieStart.getTimestamp(),latterPieStart,latterPieStart.getTimestamp()));
            return tail.prev;
        }
        return null;
    }

    public void  derivePreviousBefFromIE(LinkList<IE>.Node tail,PointEvent latterPieStart){
        while(tail!=null){
            IE lastFormerIE =  tail.getData();
            befCol.setTriggerMSG( new IEP(TemporalRelations.AllenRel.BEFORE,mpp.getFormerPred(),mpp.getLatterPred(),
                    lastFormerIE.getStartEvent(),latterPieStart,lastFormerIE.getEndEvent(),null,
                    lastFormerIE.getStartTime(),latterPieStart.getTimestamp(),latterPieStart,latterPieStart.getTimestamp() )   );
            tail = tail.prev;
        }
    }


    private LinkList<IE>.Node  deriveClosestAftIEP(PointEvent formerPieStart) {
        LinkList<IE>.Node tail = latterIEList.getTail();
        if (tail != null) {
            IE lastLatterIE = tail.getData();
            // find the last LatterIE that can just satisfy before();
            // we can sure that : lastLatterIE.getStartTime()<=formerPieStart.getTimestamp()
            if(lastLatterIE.getStartTime()==formerPieStart.getTimestamp() ){ // starts , started-by
                tail=tail.prev;
            }else if(lastLatterIE.getEndTime()==null ){  // lastLatterIE is not compeleted
                tail=tail.prev;
            }
            else if(lastLatterIE.getEndTime() == formerPieStart.getTimestamp()) {  //  met-by
                tail=tail.prev;
            }
            else { // follow  理想情况
            }
            lastLatterIE = tail.getData();
            aftCol.setTriggerMSG(new IEP(TemporalRelations.AllenRel.AFTER, mpp.getFormerPred(), mpp.getLatterPred(),
                    formerPieStart, lastLatterIE.getStartEvent(), null, lastLatterIE.getEndEvent(),
                    formerPieStart.getTimestamp(), lastLatterIE.getStartTime(), formerPieStart, formerPieStart.getTimestamp()));
            return tail.prev;
        }
        return null;
    }

    public void  derivePreviousAftFromIE(LinkList<IE>.Node tail,PointEvent formerPieStart){
        while(tail!=null){
            IE lastLatterIE =  tail.getData();
            aftCol.setTriggerMSG( new IEP(TemporalRelations.AllenRel.AFTER,mpp.getFormerPred(),mpp.getLatterPred(),
                    formerPieStart,lastLatterIE.getStartEvent(),null,lastLatterIE.getEndEvent(),
                    formerPieStart.getTimestamp(),lastLatterIE.getStartTime(),formerPieStart,formerPieStart.getTimestamp()));
            tail = tail.prev;
        }
    }

    public double getAVGprocessTime(){
        return (double)this.processTime/(double)this.resCount;
    }

}