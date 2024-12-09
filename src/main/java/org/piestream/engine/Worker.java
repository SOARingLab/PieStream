package org.piestream.engine;

import org.piestream.events.PointEvent;
import org.piestream.merger.TreeNode;
import org.piestream.parser.MPIEPairSource;
import org.piestream.piepair.eba.EBA;
import org.piestream.merger.BinTree;

import java.util.List;
import java.util.Map;



public class Worker {

    private final MPIEPairsManager mpiEPairsManager;  // MPIEPairs 管理器
    private final List<MPIEPair> MPPS;  // 所有的 PIEPair 对象
    private final Map<MPIEPairSource, TreeNode> source2Node ;  // 源到节点的映射
    private final  BinTree tree;
    private final Window  window;

    // 构造函数，创建树并映射源到节点
    public Worker(List<MPIEPairSource> MPPSourceList,Window window, Map<EBA,String> EBA2String) {

        this.window=window;

        this.tree=new BinTree(MPPSourceList,window, EBA2String);

        try {
            tree.constructTree();
            System.err.println("Tree constructed successfully.");
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to construct tree");
        }
        // 初始化 MPIEPairsManager 并获取所有的 PIEPair 对象
//        this.source2Col=tree.getSource2Col();
        this.source2Node=tree.getSourceToNode();
        this.mpiEPairsManager = new MPIEPairsManager(MPPSourceList, source2Node);
        this.MPPS = mpiEPairsManager.getMPIEPairList();

    }

    public BinTree getTree(){
        return tree;
    }

    // 获取 MPIEPairsManager
    public MPIEPairsManager getMpiEPairsManager() {
        return mpiEPairsManager;
    }

    // 处理事件，通过 MPIEPairsManager 来处理事件
    public void processEvent(Object event) {
        mpiEPairsManager.runByPE((PointEvent) event);
    }


    public void printResultCNT() {
        tree.printResultCNT();
    }

    public long getResultCNT() {
        return tree.getResultCNT();
    }


    public void printDetailResult() {
        tree.printDetailResult();
    }

    public void printResultFormat() {
        tree.printDetailResultFormat();
    }

    public void printResultOrdered() {
        tree.printDetailResultOrdered();
    }



    public void printAllTable() {
//        root.getTable().printTable();
    }

    public void resetBeforeRun(long currentTime){

        tree.clearMergedNodeData_NewT();
        tree.clearLeafNodeData_NewT();

        if(window.getWindowType()==WindowType.TIME_WINDOW){
            long deadLine=currentTime-window.getWindowCapacity();
            tree.refreshMergedNodeData_OldT(deadLine );
            tree.refreshLeafNodeData_OldT(deadLine);
        }

    }

    public void mergeAfterRun(){
        tree.mergeTree();  // 使用包装类内部的 node
    }

    public void deriveBeforeAfterRel(){
        tree.deriveBeforeAfterRel();  // 使用包装类内部的 node
    }


    public void updateData(){

        tree.updateMergedNodeData();
        tree.updateLeafNodeData();

    }



    // 逐个执行 PIEPair 的 stepByPE 方法，处理 PointEvent
    public void runOneByOne(PointEvent pe) {
        MPPS.forEach(pair -> pair.run(pe));
    }

}
