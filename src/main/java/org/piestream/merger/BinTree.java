package org.piestream.merger;

import org.piestream.engine.Window;
import org.piestream.events.Expirable;
import org.piestream.parser.MPIEPairSource;
import org.piestream.piepair.IE;
import org.piestream.piepair.IEP;
import org.piestream.piepair.eba.EBA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.TreeSet;
import java.util.Set;

import static java.lang.Math.min;

public class BinTree {
    private static final Logger logger = LoggerFactory.getLogger(BinTree.class);
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

    public long refreshNewIepTable=0;
    public long joinTime=0;
//    public long concat=0;
//    public long update_merged=0;
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

    private boolean isNodeTrigger(TreeNode node){
        return node.getCol().getIsTrigger() || (node.getBefCol()!=null && node.getBefCol().getIsTrigger() )
                ||(node.getAftCol()!=null && node.getAftCol().getIsTrigger());
    }
    public void megreBottomLeaf(){
        // bottomLeaf
        refreshNewIepTable_Leaf(bottomLeaf);
        TreeNode pn = bottomLeaf.parent;
        if(pn==root){   // mpp = 1
            pn.newT.concatenate(bottomLeaf.getCol().getNewIEPTable());    // directly concat -> finish
        }else{
            pn.newT.concatenate(bottomLeaf.getCol().getNewIEPTable(),     // concat with Rebuilding index -> continue
                    node2JoinedCols.get(pn),
                    node2JoinedCols.get(bottomLeaf));
        }
    }

    public void mergeTree() {
        megreBottomLeaf();
        TreeNode mergedNode = bottomLeaf.parent;
        TreeNode pn = mergedNode.parent;
        TreeNode leafNode = mergedNode.brother;

        while (pn != null) {    // merge Intermediate node
            getNewTabeOfLeafAndMergedNode(pn, mergedNode, leafNode);
            mergedNode = pn;
            pn = pn.parent;
            leafNode = mergedNode.brother;
        }
    }

    public void mergeTreeWithDeriving() {

        megreBottomLeaf();
        TreeNode mergedNode = bottomLeaf.parent;
        TreeNode pn = mergedNode.parent;
        TreeNode leafNode = mergedNode.brother;

        while (pn != null) {    // merge Intermediate node

            mergeIntermedNodeWithDeriving(pn, mergedNode, leafNode);
            mergedNode = pn;
            pn = pn.parent;
            leafNode = mergedNode.brother;
        }
    }

    private void deriveBottomLeaf(){

    }

    // join 之前将 本轮次的匹配结果（包括before 和 after ）全部统一到叶子节点的 col下的NewTable中
    // // updateNewIepTo Col.NewT
    private void refreshNewIepTable_Leaf(TreeNode leaf){

        leaf.getCol().updateNewIepList2Table( EBA2String,  node2JoinedCols.get(leaf));

        if (leaf.isHasBefore()){
            startTime= System.currentTimeMillis();
            leaf.getBefCol().updateNewIepList2Table( EBA2String,  node2JoinedCols.get(leaf));

            endTime= System.currentTimeMillis();
            leaf.getCol().mergeBefAftCol(leaf.getBefCol());

        }

        if (leaf.isHasAfter()){
            leaf.getAftCol().updateNewIepList2Table( EBA2String,  node2JoinedCols.get(leaf));
            leaf.getCol().mergeBefAftCol(leaf.getAftCol());
        }

        refreshNewIepTable+=(endTime-startTime);
    }

    // updateNewIepTo Col.NewT , BefCol.NewT, AftCol.NewT
    private void updateNewIepToTable(TreeNode leaf){
        leaf.getCol().updateNewIepList2Table( EBA2String,  node2JoinedCols.get(leaf));
        if (leaf.isHasBefore()  ){
            leaf.getBefCol().updateNewIepList2Table( EBA2String,  node2JoinedCols.get(leaf));
        }
        if (leaf.isHasAfter()){
            leaf.getAftCol().updateNewIepList2Table( EBA2String,  node2JoinedCols.get(leaf));
        }
    }




    public Set<String> getStringFromEBA(Set<EBA> ebaSet){
        Set<String> strs= new TreeSet<>();
        for (EBA eba : ebaSet){
            strs.add(EBA2String.get(eba));
        }
        return strs;
    }

    private void mergeIntermedNodeWithDeriving(TreeNode ParentNode, TreeNode mergedNode, TreeNode leafNode) {

        updateNewIepToTable(leafNode);
        Table intermNewTab=mergedNode.getNewT();
        Table intermOldTab=mergedNode.getT();
        Table leafOldTab=leafNode.getCol().getIEPTable();


        // for result not in BefCol.NewT and AftCol.NewT
        // Normal
        incrementalNaturalJoin(ParentNode,intermNewTab,intermOldTab,leafNode.getCol().getNewIEPTable(),leafOldTab);

        // for result in BefCol.NewT
        if(leafNode.hasBefore  ) {
            Set<EBA> keyPredSet=  leafNode.parent.keyPredSet;
            EBA formerPie= leafNode.mpp.getFormerPred();
            EBA latterPie= leafNode.mpp.getLatterPred();

            // 3. JoinOn Both  [ first join on latterPie, then derive formerPie according to formalIEList and intermTable
            if (keyPredSet.contains(formerPie) &&  keyPredSet.contains(latterPie) ){
                increDeriveFromIntermAndIEListJoin(ParentNode,formerPie,latterPie,intermNewTab,intermOldTab, leafNode.getBefCol().getNewIEPTable(),leafNode.getBefCol().getIEPTable() );
            }
            // 1. JoinOn formerPie [ first traverse intermTable, then derive formerPie according to intermTable
            else if(leafNode.parent.keyPredSet.contains(leafNode.mpp.getFormerPred())){
                increDeriveFromIntermJoin(ParentNode,formerPie,latterPie,intermNewTab,intermOldTab, leafNode.getBefCol().getNewIEPTable(),leafNode.getBefCol().getIEPTable() );
            }
            // 2. JoinOn latterPie [ first join on latterPie, then derive formerPie according to formalIEList
            else if (leafNode.parent.keyPredSet.contains(leafNode.mpp.getLatterPred()) ){
                increDeriveFromIEListJoin(ParentNode,latterPie,formerPie,intermNewTab,intermOldTab, leafNode.getBefCol().getNewIEPTable(),leafNode.getBefCol().getIEPTable(),leafNode.formerIEList );
            }
            else{
                throw new IllegalStateException("Unexpected state: No matching condition for join logic.");
            }
              }
    }

    private void increDeriveFromIntermAndIEListJoin(TreeNode ParentNode,EBA earlyPie,EBA laterPie,Table intermNewTab, Table intermOldTab,Table leafNewTab,Table leafOldTab){
        long intermNewSize=intermNewTab.getSize();
        long leafNewSize=leafNewTab.getSize();
        if( leafNewSize>1){
            throw new IllegalArgumentException("leafNewSize should not be greater than 1. Found: " + leafNewSize);
        }
        LinkList<Row>.Node leafNewPtr=leafNewTab.getRows().getHead();
        LinkList<Row>.Node leafOldPtr=leafOldTab.getRows().getHead();
        String earlyCol=EBA2String.get(earlyPie)+".ST";
        String laterCol= EBA2String.get(laterPie)+".ST";

        if(leafNewSize!=0){
            Row leafRow= leafNewTab.getRows().getHead().getData();
            // intermedia.New JOIN leaf.New (N:1)
            deriveIntermAndIEListAndJoin_IntermTab_LeafRow(ParentNode,earlyCol,laterCol, intermNewTab,leafRow);
            // intermedia.Old JOIN leaf.New (N:1)
            deriveIntermAndIEListAndJoin_IntermTab_LeafRow(ParentNode,earlyCol,laterCol, intermOldTab,leafRow);
        } if(intermNewSize!=0 && leafOldTab.getSize()!=0 ){
            // intermedia.New JOIN leaf.Old (N:K)
            // 分成 K 个 (N:1)
            LinkList<Row>.Node leafRowPtr =leafOldTab.getRows().getTail();
            while(leafRowPtr!=null  ){
                deriveIntermAndIEListAndJoin_IntermTab_LeafRow(ParentNode,earlyCol,laterCol,intermNewTab,leafRowPtr.getData());
                leafRowPtr=leafRowPtr.prev;
            }
        }
    }

    private void deriveIntermAndIEListAndJoin_IntermTab_LeafRow(TreeNode ParentNode,String earlyCol,String laterCol,Table intermTab, Row leafRow  ){
        LinkList<Row>.Node intermPtr =intermTab.getRows().getTail();
        while(intermPtr!=null && intermPtr.getData().getTimeData().get(laterCol) >= leafRow.getTimeData().get(laterCol) ){
            Row intermRow =intermPtr.getData();
            if(intermRow.getTimeData().get(laterCol) .equals(leafRow.getTimeData().get(laterCol)) ){ // find on FullKey
                // notFullkey 小于等于才可以推导
                if(intermRow.getTimeData().get(earlyCol).longValue() <= leafRow.getTimeData().get(earlyCol).longValue()){
                    Row reindexRow=new Row(intermRow.getTimeData(),node2JoinedCols.get(ParentNode),intermRow.getSource(),intermRow.getTriggerTime(),ParentNode!=root);
                    ParentNode.newT.addRow(reindexRow);
                }
            }
            intermPtr=intermPtr.prev;
        }
    }

    private void increDeriveFromIEListJoin(TreeNode ParentNode,EBA joinedPie,EBA unJoinedPie,Table intermNewTab, Table intermOldTab,Table leafNewTab,Table leafOldTab,LinkList<IE> earlyIEList){
        long intermNewSize=intermNewTab.getSize();
        long leafNewSize=leafNewTab.getSize();
        if( leafNewSize>1){
            throw new IllegalArgumentException("leafNewSize should not be greater than 1. Found: " + leafNewSize);
        }
        String unJoinedCol= EBA2String.get(unJoinedPie)+".ST";
        if(leafNewSize!=0){
            Row leafRow= leafNewTab.getRows().getHead().getData();
            // intermedia.New JOIN leaf.New (N:1)
            deriveIEListAndJoin_IntermTab_LeafRow(ParentNode,earlyIEList ,unJoinedCol, intermNewTab,leafRow);
            // intermedia.Old JOIN leaf.New (N:1)
            deriveIEListAndJoin_IntermTab_LeafRow(ParentNode,earlyIEList,unJoinedCol, intermOldTab,leafRow);
        } if(intermNewSize!=0 && leafOldTab.getSize()!=0 ){
            // intermedia.New JOIN leaf.Old (N:K)
            // 分成 K 个 (N:1)
            LinkList<Row>.Node leafRowPtr =leafOldTab.getRows().getTail();
            while(leafRowPtr!=null  ){
                deriveIEListAndJoin_IntermTab_LeafRow(ParentNode,earlyIEList,unJoinedCol, intermNewTab,leafRowPtr.getData());
                leafRowPtr=leafRowPtr.prev;
            }
        }
    }
    private void deriveIEListAndJoin_IntermTab_LeafRow(TreeNode ParentNode, LinkList<IE>  earlyIEList, String unJoinedCol, Table intermTab, Row leafRow  ){
        String leafIndex=leafRow.getIndexKey();
        List<Row> intermRows =intermTab.getHashIndex().get(leafIndex);
        if( intermRows!=null  ){
            for (Row intermRow:intermRows ){
                Row joinedRow=intermRow.join(leafRow,node2JoinedCols.get(ParentNode),ParentNode!=root );
                ParentNode.newT.addRow(joinedRow);
                // join on fullDataKey and derive in IEList ( 1:1 => 1:M )
                LinkList<IE>.Node ieNode=earlyIEList.getTail();
                // 跳过可能已经合并的
                while(ieNode!=null && ieNode.getData().getStartTime()>=leafRow.getTimeData().get(unJoinedCol)){
                    ieNode=ieNode.prev;
                }
                long derivedIEcnt=0;
                while(ieNode!=null){
                    // join on ieNode
                    joinRowWithIE(ParentNode,intermRow,unJoinedCol,ieNode.getData());
                    ieNode=ieNode.prev;
                    derivedIEcnt++;
                }
                if(derivedIEcnt>0){
                    logger.debug("derivedIEcnt: "+derivedIEcnt);
                }
            }
        }
    }

    // Build a Row with joining two row: intermRow is base , add addedPieName of leafRow
    private void joinRowWithIE(TreeNode ParentNode, Row intermRow , String addedPieName, IE ie ){
        Map<String,Long> timeData= new HashMap<>(intermRow.getTimeData()) ;
        timeData.put(addedPieName,ie.getStartTime());
        Set<Expirable> source= new HashSet<>(intermRow.getSource());
        source.add(ie);
        long minTrig=min(ie.getStartTime(),intermRow.getTriggerTime());
        Row newRow = new Row(timeData,node2JoinedCols.get(ParentNode),source,minTrig,ParentNode!=root );
        ParentNode.newT.addRow(newRow);
    }

    private void increDeriveFromIntermJoin(TreeNode ParentNode,EBA joinedPie,EBA unJoinedPie,  Table  intermNewTab, Table intermOldTab,Table leafNewTab,Table leafOldTab ){
        long intermNewSize=intermNewTab.getSize();
        long leafNewSize=leafNewTab.getSize();
        if( leafNewSize>1){
            throw new IllegalArgumentException("leafNewSize should not be greater than 1. Found: " + leafNewSize);
        }
        LinkList<Row>.Node leafNewPtr=leafNewTab.getRows().getHead();
        LinkList<Row>.Node leafOldPtr=leafOldTab.getRows().getHead();
        String joinedCol=EBA2String.get(joinedPie)+".ST";
        String unJoinedCol= EBA2String.get(unJoinedPie)+".ST";
        if(leafNewSize!=0){
            // intermedia.New JOIN leaf.New (N:1)
            deriveIntermAndJoin_Interm_LeafNew(ParentNode,joinedCol,unJoinedCol, intermNewTab,leafNewPtr);
            // intermedia.Old JOIN leaf.New (N:1)
            deriveIntermAndJoin_Interm_LeafNew(ParentNode,joinedCol,unJoinedCol, intermOldTab,leafNewPtr);
        }
        if(intermNewSize!=0 && leafOldTab.getSize()!=0 ){
            // intermedia.New JOIN leaf.Old (N:M)
            deriveIntermAndJoin_IntermNew_LeafOld(ParentNode,intermNewTab,joinedCol,unJoinedCol,leafOldPtr);
        }
    }
    private void deriveIntermAndJoin_Interm_LeafNew(TreeNode ParentNode, String joinedCol, String unJoinedCol,  Table intermTab, LinkList<Row>.Node leafPtr ){

        if(intermTab.getSize() !=0){
            LinkList<Row>.Node intermNode=intermTab.getRows().getHead();
            while(intermNode!=null){
                long intermTime = intermNode.getData().getTimeData().get(joinedCol);
                if( intermTime<=leafPtr.getData().getTimeData().get(joinedCol)){
                    //  at most once every round
                    joinTwoRows( ParentNode, intermNode.getData(),unJoinedCol,leafPtr.getData());
                }
                intermNode=intermNode.next;
            }
        }

    }

    private void deriveIntermAndJoin_IntermNew_LeafOld(TreeNode ParentNode, Table intermTab, String joinedCol, String unJoinedCol, LinkList<Row>.Node leafPtr ){

        if(intermTab.getSize() !=0){
            LinkList<Row>.Node intermNode=intermTab.getRows().getHead();
            while(intermNode!=null){
                long intermTime = intermNode.getData().getTimeData().get(joinedCol);
                if( intermTime<=leafPtr.getData().getTimeData().get(joinedCol)){
                    // first : find the steer , search back
                    while(leafPtr!=null && leafPtr.getData().getTimeData().get(joinedCol) >intermTime  ){
                        leafPtr=leafPtr.prev;
                    }
                    // second : build the rows , build forword
                    while(leafPtr!=null){
                        joinTwoRows( ParentNode, intermNode.getData(),unJoinedCol,leafPtr.getData());
                        leafPtr=leafPtr.next;
                    }
                }
                intermNode=intermNode.next;
            }
        }
    }

    // Build a Row with joining two row: intermRow is base , add addedPieName of leafRow
    private void joinTwoRows(TreeNode ParentNode, Row intermRow , String addedPieName, Row leafRow ){
        Map<String,Long> timeData= new HashMap<>(intermRow.getTimeData()) ;
        timeData.put(addedPieName,leafRow.getTimeData().get(addedPieName));
        Set<Expirable> source= new HashSet<>(intermRow.getSource())  ;
        source.addAll(leafRow.getSource());
        long minTrig=min(leafRow.getTriggerTime(),intermRow.getTriggerTime());
        Row newRow = new Row(timeData,node2JoinedCols.get(ParentNode),source,minTrig,ParentNode!=root );
        ParentNode.newT.addRow(newRow);
    }

    private void incrementalNaturalJoin(TreeNode ParentNode, Table  intermNewTab, Table intermOldTab,Table leafNewTab,Table leafOldTab ){
        long mergedNewSize=intermNewTab.getSize();
        long leafNewSize=leafNewTab.getSize();
        if(mergedNewSize!=0 ){
            // intermedia.New JOIN  leaf.Old
            Table IntermNew_LeafOld=HashJoiner.hashJoin(intermNewTab, leafOldTab ,node2JoinedCols.get(ParentNode),ParentNode!=root );
            ParentNode.newT.concatenate( IntermNew_LeafOld ) ;
        }
        if( leafNewSize!=0){
            // intermedia.Old JOIN leaf.New
            Table IntermOld_LeafNew = HashJoiner.hashJoin( leafNewTab,intermOldTab ,node2JoinedCols.get(ParentNode),ParentNode!=root);
            ParentNode.newT.concatenate(IntermOld_LeafNew);
        }
        if(mergedNewSize*leafNewSize!=0){
            // intermedia.New JOIN leaf.New
            Table IntermNew_LeafNew= HashJoiner.hashJoin(intermNewTab, leafNewTab,node2JoinedCols.get(ParentNode),ParentNode!=root);
            ParentNode.newT.concatenate(IntermNew_LeafNew);
        }
    }



    // 将中间结点的信息进行更新
    private void getNewTabeOfLeafAndMergedNode(TreeNode ParnetNode, TreeNode mergedNode, TreeNode leafNode) {
        refreshNewIepTable_Leaf(leafNode);
        Table mergedNewTab = mergedNode.getNewT();
        Table leafNewTab = leafNode.getCol().getNewIEPTable();
        long mergedNewSize=mergedNewTab.getSize();
        long leafNewSize=leafNode.getCol().getNewIEPTable().getSize();

        if(mergedNewSize!=0 ){
            // intermedia.New JOIN  leaf.Old
            Table IntermNew_LeafOld=HashJoiner.hashJoin(mergedNewTab,  leafNode.getCol().getIEPTable(),node2JoinedCols.get(ParnetNode),ParnetNode!=root );
            ParnetNode.newT.concatenate( IntermNew_LeafOld ) ;
        }
        if( leafNewSize!=0){
            // intermedia.Old JOIN leaf.New
            Table IntermOld_LeafNew = HashJoiner.hashJoin( leafNewTab, mergedNode.getT(),node2JoinedCols.get(ParnetNode),ParnetNode!=root);
            ParnetNode.newT.concatenate(IntermOld_LeafNew);
        }
        if(mergedNewSize*leafNewSize!=0){
            // intermedia.New JOIN leaf.New
            Table IntermNew_LeafNew= HashJoiner.hashJoin(mergedNewTab, leafNewTab,node2JoinedCols.get(ParnetNode),ParnetNode!=root);
            ParnetNode.newT.concatenate(IntermNew_LeafNew);
        }
    }

    public void deriveBeforeAfterRel() {
        for (Map.Entry<IEPCol, TreeNode> entry : Col2Node.entrySet()) {
            //  derive only within LeafNode
            TreeNode node=entry.getValue();
            // bottomLeaf need Derive All Previous IEP,
            // while other leaf node derive key IEP and the rest will be derived in Merge stage to reduce calculation.
            node.deriveBeforeAfterRel(node==bottomLeaf);
        }
    }

    public void printResultCNT() {
        long cnt = root.getResCount();
        logger.info("RESULT:" + cnt);
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
    }

    public void updateLeafNodeData() {
        startTime = System.currentTimeMillis();
        for (Map.Entry<IEPCol, TreeNode> entry : Col2Node.entrySet()) {
            TreeNode node = entry.getValue();
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
                leafNode.formerIEList.refresh(deadLine);
                toDelIeps=leafNode.getBefCol().refresh(deadLine);
//                updateIepET2RootTable(toDelIeps);
            }
            if (leafNode.isHasAfter()) {
                leafNode.latterIEList.refresh(deadLine);
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
