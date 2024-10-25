package org.example.merger;

import org.example.parser.Schema;
import org.example.parser.MPIEPairSource;
import org.example.parser.QueryParser;
import org.example.piepair.eba.EBA;

import java.util.*;

public class BinTree {

    private List<MPIEPairSource> sourceList;
    private Map<MPIEPairSource, IEPCol> source2Col;
    private Map<MPIEPairSource, TreeNode> sourceToNode;
    private Map<IEPCol, TreeNode> Col2Node;
    private long QCapacity;
    private long bef_aftQCapacity;
    private TreeNode root;
    private TreeNode bottomLeaf;
    private List<TreeNode> mergedNodes;
    private Map<EBA, String> EBA2String;
    private Map<TreeNode, List<String>> node2JoinedCols;

    // 构造函数初始化常用的变量
    public BinTree(List<MPIEPairSource> sourceList, long QCapacity, Map<EBA, String> EBA2String) {
        this.sourceList = sourceList;
        this.QCapacity = QCapacity;
        this.source2Col = new HashMap<>();
        this.sourceToNode = new HashMap<>();
        this.Col2Node = new HashMap<>();
        this.root = null;
        this.bottomLeaf = null;
        this.EBA2String = EBA2String;
        this.bef_aftQCapacity = ((QCapacity + 3) * QCapacity) / 6;
        this.mergedNodes = new ArrayList<>();
        this.node2JoinedCols=new HashMap<>();
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
            if (left!=null ) {
//                left.setJoinedCols(joinedCols);
                this.node2JoinedCols.put(left,joinedCols);
            }
            if (right!=null ) {
//                right.setJoinedCols(joinedCols);
                this.node2JoinedCols.put(right,joinedCols);
            }

            buildJoinedCols(left);
            buildJoinedCols(right);
        }
    }
    // 创建叶子节点
    private List<TreeNode> createLeafNodes() {
        List<TreeNode> nodes = new ArrayList<>();
        for (MPIEPairSource source : sourceList) {
            Set<EBA> predSet = new HashSet<>();
            predSet.add(source.getFormerPred()); // 添加前驱谓词
            predSet.add(source.getLatterPred()); // 添加后继谓词
            // 创建叶子节点
            IEPCol Col = new IEPCol(QCapacity,  EBA2String);
            // TreeNode node = new TreeNode(predSet, new HashSet<>(), null, null, null,
            // null, source, 0, true, null, Col);
            TreeNode node = new TreeNode(predSet, source, QCapacity,  EBA2String);

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
        TreeNode mergedNode = createParentNode(hNode, null, heightCnt); // 合并两个节点
        hNode = mergedNode;
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
        TreeNode mergedNode = new TreeNode(newPredSet, newKeyPredSet, hNode, otherNode, heightCnt + 1, QCapacity,  EBA2String);

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
            pn.newT.concatenate(leafNode.getCol().getNewIEPTable(),
                    node2JoinedCols.get(pn),
                    node2JoinedCols.get(leafNode));
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

    // 将叶子结点的信息进行更新
//    private void getNewTableOfLeafNode(TreeNode leafNode) {
//
//        // leafNode.leafNewT.concatenate( new
//        // Table(leafNode.getCol().getNewIEPList(),EBA2String ) );
//        // if (leafNode.isHasBefore()){
//        // leafNode.leafNewT.concatenate( new Table(
//        // leafNode.getBefCol().getNewIEPList(),EBA2String ) );
//        // }
//        // if (leafNode.isHasAfter()){
//        // leafNode.leafNewT.concatenate( new Table(
//        // leafNode.getAftCol().getNewIEPList(),EBA2String ) );
//        // }
//
//        leafNode.leafNewT.concatenateList( leafNode.getCol().getNewIEPList(), EBA2String);
//        if (leafNode.isHasBefore()) {
//            leafNode.leafNewT.concatenateList(leafNode.getBefCol().getNewIEPList(), EBA2String);
//        }
//        if (leafNode.isHasAfter()) {
//            leafNode.leafNewT.concatenateList(leafNode.getAftCol().getNewIEPList(), EBA2String);
//        }
//
//    }

    private void refreshNewIepTable_Leaf(TreeNode leaf){
        leaf.getCol().updateNewIepList2Table( EBA2String,  node2JoinedCols.get(leaf));
    }

    // 将中间结点的信息进行更新
    private void getNewTabeOfLeafAndMergedNode(TreeNode ParnetNode, TreeNode mergedNode, TreeNode leafNode) {

        //        getNewTableOfLeafNode(leafNode);
        refreshNewIepTable_Leaf(leafNode);
        Table mergedNewTab = mergedNode.getNewT();

        if(mergedNewTab.getRowCount()!=0){
            Table t1= HashJoiner.hashJoin(mergedNewTab, leafNode.getCol().getNewIEPTable(),node2JoinedCols.get(ParnetNode));
            ParnetNode.newT.concatenate(t1);
            Table t2=HashJoiner.hashJoin(mergedNewTab,  leafNode.getCol().getIEPTable(),node2JoinedCols.get(ParnetNode) );
            ParnetNode.newT.concatenate( t2 ) ;
        }
        if( leafNode.getCol().getNewIEPTable().getRowCount()!=0){
            Table tb = HashJoiner.hashJoin( leafNode.getCol().getNewIEPTable(), mergedNode.getT(),node2JoinedCols.get(ParnetNode));
            ParnetNode.newT.concatenate(tb);
        }
    }

    public void deriveBeforeAfterRel() {
        for (Map.Entry<IEPCol, TreeNode> entry : Col2Node.entrySet()) {
            // 只对叶子结点推导

            entry.getValue().deriveBeforeAfterRel();

        }
    }

    public void printResultCNT() {
        long cnt = root.T.getRowCount();
        System.out.println("RESULT:" + cnt);
    }

    public long getResultCNT() {
        return root.T.getRowCount();

    }

    public void printDetailResult() {
        root.T.printTable();
    }

    public void printDetailResultFormat() {
        root.T.printTableFormat();
    }

    public void printDetailResultOrdered() {
        root.T.printTableOrdered();
    }

    public void updateMergedNodeData() {
        for (TreeNode node : mergedNodes) {
            node.T.concatenate(node.newT);
        }
    }

    public void updateLeafNodeData() {

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

    }

    public void clearLeafNodeData() {
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

    public void clearMergedNodeData() {
        for (TreeNode node : mergedNodes) {
            Table newT = node.newT;
            if (newT.getRowCount() != 0) {
                newT.clear();
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

    public long getQCapacity() {
        return QCapacity;
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

    // 主方法测试
    public static void main(String[] args) {
        String query = "SELECT s.ts, s.te " +
                "FROM CarStream " +
                "DEFINE l AS Lane > 2, s AS SPEED > 30 , x AS XWay > 2 " +
                "PATTERN s follow;followed-by;meets;met-by;overlapped-by;overlaps;started-by;starts;during;contains;finishes;finished-by;equals l "
                +
                "AND x contains l " +
                "AND s overlaps x " +
                "WINDOW 5 min";

        // 解析查询
        String schemaFilePath = "src/main/resources/domain/linear_accel.yaml";
        Schema schema = new Schema(schemaFilePath); // 加载 Schema

        QueryParser parser = new QueryParser(query, schema);
        try {
            parser.parse();
        } catch (QueryParser.ParseException | EBA.ParseException e) {
            System.err.println("解析查询失败: " + e.getMessage());
            return;
        }

        List<MPIEPairSource> sources = parser.getPatternClause(); // 获取解析后的模式源

        System.out.println("解析后的 MPIEPairSource:");
        for (MPIEPairSource source : sources) {
            System.out.println("FormerPred: " + source.getFormerPred() + ", LatterPred: " + source.getLatterPred());
        }

        int QCapacity = 100;

        // 创建 BinTree 实例
        BinTree binTree = new BinTree(sources, QCapacity, parser.getEBA2String());

        try {
            TreeNode root = binTree.constructTree(); // 构建二叉树
            System.err.println("Build BinTree successfully!");
        } catch (Exception e) {
            System.err.println("Build BinTree failed! " + e.getMessage());
        }
    }
}
