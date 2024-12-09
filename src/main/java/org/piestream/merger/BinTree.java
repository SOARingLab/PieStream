package org.piestream.merger;

import org.piestream.engine.Window;
import org.piestream.parser.MPIEPairSource;
import org.piestream.piepair.IEP;
import org.piestream.piepair.eba.EBA;

import java.util.*;
import java.util.TreeSet;
import java.util.Set;

public class BinTree {

    private final List<MPIEPairSource> sourceList;
    private final Map<MPIEPairSource, IEPCol> source2Col;
    private final Map<MPIEPairSource, TreeNode> sourceToNode;
    private final Map<IEPCol, TreeNode> Col2Node;
    private TreeNode root;
    private TreeNode bottomLeaf;
    private final List<TreeNode> mergedNodes;
    private final Map<EBA, String> EBA2String;
    private final Window window;
    private final Map<TreeNode, List<String>> node2JoinedCols;

    private final Map<TreeNode, Set<String>> node2AcmltJoinedCols;

    public long joinTime=0;
    public long concat=0;
    public long update_merged=0;
    public long update_leaf=0;
    public long startTime=0;
    public long endTime=0;


    public BinTree(List<MPIEPairSource> sourceList, Window window, Map<EBA, String> EBA2String) {

        this.sourceList = sourceList;
        this.window = window;
        this.source2Col = new HashMap<>();
        this.sourceToNode = new HashMap<>();
        this.Col2Node = new HashMap<>();
        this.root = null;
        this.bottomLeaf = null;
        this.EBA2String = EBA2String;
        this.mergedNodes = new ArrayList<>();
        this.node2JoinedCols=new HashMap<>();
        this.node2AcmltJoinedCols=new HashMap<>();
    }


        // 构建二叉树的方法
    public TreeNode constructTree() throws Exception {
        List<TreeNode> nodes = createLeafNodes(); // Create leaf nodes from sources

        // 如果是空树，抛出异常
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("No nodes to merge");
        }

        root = createTree(nodes); // Merge the nodes into a binary tree

        //构建完成之后，构建每个Tbale的 joinedCols

        this.node2JoinedCols.put(root,null);
        this.node2AcmltJoinedCols.put(root,null);
        buildJoinedCols(root);
        return root;
    }

    private void  buildJoinedCols(TreeNode node){
        if (node !=null && !node.isLeaf){
            List<String> joinedCols=new ArrayList<>();
            for (EBA eba : node.getKeyPredSet()){
                joinedCols.add(EBA2String.get(eba));
            }
            TreeNode  left= node.getLeft();
            TreeNode  right= node.getRight();
            Set<String> acmlt=new TreeSet<>();
            if(node2AcmltJoinedCols.get(node)!=null){
                acmlt.addAll(node2AcmltJoinedCols.get(node)) ;
            }
            acmlt.addAll(joinedCols);
            if (left != null) {
                this.node2AcmltJoinedCols.putIfAbsent(left, new TreeSet<>());
                CollectAcmltWithTableCols(left,new TreeSet<>(acmlt));
                // 更新 node2JoinedCols，覆盖旧值
                this.node2JoinedCols.put(left, new ArrayList<>(joinedCols));
            }
            if (right != null) {
                this.node2AcmltJoinedCols.putIfAbsent(right, new TreeSet<>());
                CollectAcmltWithTableCols(right,new TreeSet<>(acmlt));
                // 更新 node2JoinedCols，覆盖旧值
                this.node2JoinedCols.put(right, new ArrayList<>(joinedCols));
            }



            buildJoinedCols(left);
            buildJoinedCols(right);
        }
    }

    private void CollectAcmltWithTableCols(TreeNode node,Set<String> acmlt){

        Set<String> predSets = new TreeSet<>();
        for (EBA eba : node.getPredSet()){
            predSets.add(EBA2String.get(eba));
        }
        acmlt.retainAll(predSets);

        this.node2AcmltJoinedCols.put(node,acmlt);
    }
    // 创建叶子节点
    private List<TreeNode> createLeafNodes() {
        List<TreeNode> nodes = new ArrayList<>();
        for (MPIEPairSource source : sourceList) {
            Set<EBA> predSet = new HashSet<>();
            predSet.add(source.getFormerPred()); // 添加前驱谓词
            predSet.add(source.getLatterPred()); // 添加后继谓词
            // 创建叶子节点
            IEPCol Col = new IEPCol(window,   EBA2String);
            // TreeNode node = new TreeNode(predSet, new HashSet<>(), null, null, null,
            // null, source, 0, true, null, Col);
            TreeNode node = new TreeNode(predSet, source, window,  EBA2String);

            source2Col.put(source, Col);
            Col2Node.put(Col, node);
            sourceToNode.put(source, node); // 将源和节点映射
            nodes.add(node);
        }
        return nodes;
    }

    // 将节点合并成二叉树
    private TreeNode createTree(List<TreeNode> nodes) throws Exception {

        int heightCnt = 0; // 初始化高度计数器
        TreeNode hNode = nodes.remove(0); // 取第一个节点作为起始节点
        // 更新底层叶子节点
        bottomLeaf = hNode;
        hNode.height = heightCnt;
        heightCnt++;
        hNode = createParentNode(hNode, null, heightCnt);
        mergedNodes.add(hNode);

        while (!nodes.isEmpty()) {
            boolean mergedCurrent = false;
            for (int i = 0; i < nodes.size(); i++) {
                TreeNode otherNode = nodes.get(i);
                if (canMerge(hNode, otherNode)) { // 判断两个节点是否可以合并
                    if (heightCnt == 0) {
                        bottomLeaf = hNode;
                    }
                    TreeNode parentNode = createParentNode(hNode, otherNode, heightCnt); // 合并两个节点
                    heightCnt++;
                    hNode = parentNode; // 更新当前节点
                    mergedNodes.add(hNode);
                    nodes.remove(i); // 移除合并后的节点
                    mergedCurrent = true;
                    break;
                }
            }

            if (!mergedCurrent) {
                throw new Exception("二叉树构建失败");
            }
        }

        return hNode; // 返回根节点
    }

    // 判断两个节点是否可以合并
    private boolean canMerge(TreeNode hNode, TreeNode otherNode) {
        Set<EBA> intersection = new HashSet<>(hNode.predSet);
        intersection.retainAll(otherNode.predSet); // 计算交集
        return !intersection.isEmpty(); // 共享谓词的节点可以合并
    }

    // 合并两个节点并更新属性
    private TreeNode createParentNode(TreeNode hNode, TreeNode otherNode, int heightCnt) {
        Set<EBA> newPredSet = new HashSet<>(hNode.predSet); // 用 hNode 的谓词集初始化

        if (otherNode != null) {
            newPredSet.addAll(otherNode.predSet); // 合并其他节点的谓词
        }

        Set<EBA> newKeyPredSet = new HashSet<>(hNode.predSet);

        if (otherNode != null) {
            newKeyPredSet.retainAll(otherNode.predSet); // 计算交集
        }

        hNode.height = heightCnt + 1;

        if (otherNode != null) {
            otherNode.height = heightCnt + 1;
            otherNode.brother = hNode;
            hNode.brother = otherNode;
        }

        // TreeNode mergedNode = new TreeNode(newPredSet, newKeyPredSet, hNode,
        // otherNode, null, null, null, heightCnt + 1, false, pt, null);
        TreeNode mergedNode = new TreeNode(newPredSet, newKeyPredSet, hNode, otherNode, heightCnt + 1,   window,  EBA2String);

        hNode.parent = mergedNode;
        if (otherNode != null) {
            otherNode.parent = mergedNode;
        }
        return mergedNode;
    }

    public void mergeTree() {
        // TreeNode startNode = getStartMergeNode();

        TreeNode startNode = bottomLeaf;
        if (startNode != null) {// 为null则不用更新
            mergeNewTableFrom(startNode);
        }
    }

    private TreeNode getStartMergeNode() {

        TreeNode startNode = bottomLeaf;

        if (startNode.getCol().getIsTrigger()) {
            return startNode;
        }
        TreeNode mergedNode = startNode.parent;
        startNode = mergedNode.brother;
        TreeNode pn = mergedNode.parent;
        while (pn != null) {
            if (startNode.getCol().getIsTrigger()) {
                return startNode;
            }
            mergedNode = startNode.parent;
            startNode = mergedNode.brother;
            pn = mergedNode.parent;
        }

        return null;// 不用更新

    }

    private void mergeNewTableFrom(TreeNode startNode) {
        TreeNode leafNode = startNode;
        TreeNode mergedNode = leafNode.brother;
        TreeNode pn = leafNode.parent;

        if (leafNode == bottomLeaf) { // 最底层

//            getNewTableOfLeafNode(leafNode);
            refreshNewIepTable_Leaf(leafNode);
            if(pn==root){   // mpp = 1
                pn.newT.concatenate(leafNode.getCol().getNewIEPTable());
            }else{
                pn.newT.concatenate(leafNode.getCol().getNewIEPTable(),
                        node2JoinedCols.get(pn),
                        node2JoinedCols.get(leafNode));
            }
            // pn.newT.printTable();
            mergedNode = pn;
            pn = pn.parent;
            leafNode = mergedNode.brother;
        }

        while (pn != null) {
            getNewTabeOfLeafAndMergedNode(pn, mergedNode, leafNode);
            // pn.newT.printTable();
            mergedNode = pn;
            pn = pn.parent;
            leafNode = mergedNode.brother;
        }

    }


    private void refreshNewIepTable_Leaf(TreeNode leaf){
        leaf.getCol().updateNewIepList2Table( EBA2String,  node2JoinedCols.get(leaf));
        if (leaf.isHasBefore()){
            leaf.getBefCol().updateNewIepList2Table( EBA2String,  node2JoinedCols.get(leaf));
            leaf.getCol().mergeBefAftCol(leaf.getBefCol());
        }
        if (leaf.isHasAfter()){
            leaf.getAftCol().updateNewIepList2Table( EBA2String,  node2JoinedCols.get(leaf));
            leaf.getCol().mergeBefAftCol(leaf.getAftCol());
        }
    }


    public Set<String> getStringFromEBA(Set<EBA> ebaSet){
        Set<String> strs= new TreeSet<>();
        for (EBA eba : ebaSet){
            strs.add(EBA2String.get(eba));
        }
        return strs;
    }


    // 将中间结点的信息进行更新
    private void getNewTabeOfLeafAndMergedNode(TreeNode ParnetNode, TreeNode mergedNode, TreeNode leafNode) {

        refreshNewIepTable_Leaf(leafNode);
        Table mergedNewTab = mergedNode.getNewT();

        long mergedNewTabCnt=mergedNewTab.getSize();
        long leafNewTabCnt=leafNode.getCol().getNewIEPTable().getSize();
        boolean needNewIndex=true;
        if(ParnetNode==root){
            needNewIndex=false;
        }

        if(mergedNewTab.getSize()!=0 ){
            startTime = System.currentTimeMillis();


            Table t1= HashJoiner.hashJoin(mergedNewTab, leafNode.getCol().getNewIEPTable(),node2JoinedCols.get(ParnetNode),needNewIndex);
//            Table t1= HashJoiner.hashJoin(mergedNewTab, leafNode.getCol().getNewIEPTable(), JoinedCols,node2AcmltJoinedCols.get(ParnetNode));

            Table t2=HashJoiner.hashJoin(mergedNewTab,  leafNode.getCol().getIEPTable(),node2JoinedCols.get(ParnetNode),needNewIndex );
//            Table t2=HashJoiner.hashJoin(mergedNewTab,  leafNode.getCol().getIEPTable(),JoinedCols,node2AcmltJoinedCols.get(ParnetNode) );
            endTime = System.currentTimeMillis();
            joinTime += (endTime - startTime);

            startTime = System.currentTimeMillis();
            ParnetNode.newT.concatenate(t1);
            ParnetNode.newT.concatenate( t2 ) ;
            endTime = System.currentTimeMillis();
            concat += (endTime - startTime);

        }
        if( leafNode.getCol().getNewIEPTable().getSize()!=0){

            startTime = System.currentTimeMillis();
            Table tb = HashJoiner.hashJoin( leafNode.getCol().getNewIEPTable(), mergedNode.getT(),node2JoinedCols.get(ParnetNode),needNewIndex);
            endTime = System.currentTimeMillis();
            joinTime += (endTime - startTime);
            startTime = System.currentTimeMillis();
            ParnetNode.newT.concatenate(tb);
            endTime = System.currentTimeMillis();
            concat += (endTime - startTime);
        }
    }

    public void deriveBeforeAfterRel() {
        for (Map.Entry<IEPCol, TreeNode> entry : Col2Node.entrySet()) {
            // 只对叶子结点推导

            entry.getValue().deriveBeforeAfterRel();

        }
    }

    public void printResultCNT() {
        long cnt = root.getResCount();
        System.out.println("RESULT:" + cnt);
    }

    public long getResultCNT() {
        return root.T.getSize();

    }

    public void printDetailResult() {
//        root.T.printTable();
    }

    public void printDetailResultFormat() {

//        root.T.printTableFormat();
    }

    public void printDetailResultOrdered() {

//        root.T.printTableOrdered();
    }

    public void updateMergedNodeData() {
        startTime = System.currentTimeMillis();
        for (TreeNode node : mergedNodes) {
            if(node==root){
                long time=node.newT.addDetectTimeAndCalProcessTime("detectTime",System.nanoTime());

                node.addProcessTime(time);
                node.T.concatenate(node.newT);
                node.addResCount( node.newT.getSize());
            }else{

                node.T.concatenate(node.newT);
                node.addResCount( node.newT.getSize());
            }
        }
        endTime = System.currentTimeMillis();
        update_merged+=endTime-startTime;
    }

    public void updateLeafNodeData() {
        startTime = System.currentTimeMillis();
        for (Map.Entry<IEPCol, TreeNode> entry : Col2Node.entrySet()) {
            // MPIEPairSource key = entry.getKey();
            TreeNode node = entry.getValue();

            // node.getCol().printNewIEPList();
            node.getCol().updateIEP2List();

            if (node.isHasBefore()) {
                // node.getBefCol().printNewIEPList();
                node.getBefCol().updateIEP2List();

            }
            if (node.isHasAfter()) {
                // node.getAftCol().printNewIEPList();
                node.getAftCol().updateIEP2List();
            }

        }
        endTime = System.currentTimeMillis();
        update_leaf+=endTime-startTime;

    }

    public void clearLeafNodeData_NewT() {
        for (Map.Entry<IEPCol, TreeNode> entry : Col2Node.entrySet()) {
            // MPIEPairSource key = entry.getKey();
            TreeNode leafNode = entry.getValue();
            leafNode.getCol().resetIsTrigger();
//            leafNode.leafNewT.clear();
            if (leafNode.isHasBefore()) {
                leafNode.getBefCol().resetIsTrigger();
            }
            if (leafNode.isHasAfter()) {
                leafNode.getAftCol().resetIsTrigger();
            }

        }
    }

    public void clearMergedNodeData_NewT() {
        for (TreeNode node : mergedNodes) {
            Table newT = node.newT;
            if (newT.getSize() != 0) {
                newT.clear();
            }
        }
    }

    public void refreshMergedNodeData_OldT(long deadLine ){
        for (TreeNode node : mergedNodes) {
            Table oldT = node.T;
            if (oldT.getSize() != 0) {
                oldT.refresh(deadLine );
            }
        }
    }

    public void refreshLeafNodeData_OldT(long deadLine ){
        for (Map.Entry<IEPCol, TreeNode> entry : Col2Node.entrySet()) {
            // MPIEPairSource key = entry.getKey();
            TreeNode leafNode = entry.getValue();
            List<IEP> toDelIeps=leafNode.getCol().refresh(deadLine);
            //update Pie End time before Deleted
//            updateIepET2RootTable(toDelIeps);
//            leafNode.leafNewT.clear();
            if (leafNode.isHasBefore()) {
                toDelIeps=leafNode.getBefCol().refresh(deadLine);
//                updateIepET2RootTable(toDelIeps);
            }
            if (leafNode.isHasAfter()) {
                toDelIeps=leafNode.getAftCol().refresh(deadLine);
//                updateIepET2RootTable(toDelIeps);
            }

        }
    }

    private void updateIepET2RootTable(List<IEP> toDelIeps){
        Table rootTable=root.getT();
        for (IEP iep:toDelIeps){
            if(iep.getCompTime()== IEP.CompletedTime.LatterEnd){
                rootTable.update(EBA2String.get(iep.getLatterPie()),iep.getLatterStartTime(),iep.getLatterEndTime());
            }
            else if(iep.getCompTime()== IEP.CompletedTime.FormerEnd){
                rootTable.update(EBA2String.get(iep.getFormerPie()),iep.getFormerStartTime(),iep.getFormerEndTime());

            }else{
                return;
            }
        }
    }
    // Getter 方法
    public List<MPIEPairSource> getSourceList() {
        return sourceList;
    }

    public Map<MPIEPairSource, IEPCol> getSource2Col() {
        return source2Col;
    }

    public Map<MPIEPairSource, TreeNode> getSourceToNode() {
        return sourceToNode;
    }

    public Map<IEPCol, TreeNode> getCol2Node() {
        return Col2Node;
    }


    public TreeNode getRoot() {
        return root;
    }

    public TreeNode getBottomLeaf() {
        return bottomLeaf;
    }

    public Map<EBA, String> getEBA2String() {
        return EBA2String;
    }

}
