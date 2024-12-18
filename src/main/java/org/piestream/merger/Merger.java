package org.piestream.merger;

import org.piestream.events.Expirable;
import org.piestream.piepair.IE;
import org.piestream.piepair.eba.EBA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.lang.Math.min;

public class Merger {

    private static final Logger logger = LoggerFactory.getLogger(Merger.class);
    private final TreeNode  root;
    private final TreeNode bottomLeaf;
    private final Map<TreeNode, List<String>> node2JoinedCols;
    private final Map<EBA, String> EBA2String;

    public long refreshNewIepTable=0;
    public long startTime=0;
    public long endTime=0;

    public Merger(TreeNode root, TreeNode bottomLeaf, Map<EBA,String> EBA2String, Map<TreeNode, List<String>> node2JoinedCols ) {
        this.root = root;
        this.bottomLeaf = bottomLeaf;
        this.EBA2String = EBA2String;
        this.node2JoinedCols=node2JoinedCols ;
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

            mergeIntermedNode(pn, mergedNode, leafNode);
            mergedNode = pn;
            pn = pn.parent;
            leafNode = mergedNode.brother;
        }
    }

    private void mergeIntermedNode(TreeNode ParentNode, TreeNode mergedNode, TreeNode leafNode) {

        updateNewIepToTable(leafNode);
        Table intermNewTab=mergedNode.getNewT();
        Table intermOldTab=mergedNode.getT();
        Table leafOldTab=leafNode.getCol().getIEPTable();

        // for result not in BefCol.NewT and AftCol.NewT
        // Normal
        incrementalNaturalJoin(ParentNode,intermNewTab,intermOldTab,leafNode.getCol().getNewIEPTable(),leafOldTab);

        Set<EBA> keyPredSet=  leafNode.parent.keyPredSet;
        EBA earlyPie;
        EBA laterPie;
        // for result in AfterCol.NewT
        if(leafNode.hasAfter  ) {
            earlyPie= leafNode.mpp.getLatterPred();
            laterPie= leafNode.mpp.getFormerPred();
            mergeIntermedNodeDerivingAfter(ParentNode,leafNode,keyPredSet,earlyPie, laterPie,  intermNewTab,intermOldTab);
        }
        // for result in BefCol.NewT
        if(leafNode.hasBefore  ) {
            earlyPie= leafNode.mpp.getFormerPred();
            laterPie= leafNode.mpp.getLatterPred();
            mergeIntermedNodeDerivingBefore(ParentNode,leafNode,keyPredSet,earlyPie, laterPie,  intermNewTab,intermOldTab);
        }

    }

    private  void mergeIntermedNodeDerivingBefore(TreeNode ParentNode,TreeNode leafNode,Set<EBA> keyPredSet,EBA earlyPie, EBA laterPie, Table  intermNewTab, Table intermOldTab){

        EBA joinedPie;
        EBA unJoinedPie;

        // 3. JoinOn Both  [ first join on latterPie, then derive formerPie according to formalIEList and intermTable
        if (keyPredSet.contains(earlyPie) &&  keyPredSet.contains(laterPie) ){
            increDeriveFromIntermAndIEListJoin(ParentNode,earlyPie,laterPie,intermNewTab,intermOldTab, leafNode.getBefCol().getNewIEPTable(),leafNode.getBefCol().getIEPTable() );
        }
        // 1. JoinOn formerPie [ first traverse intermTable, then derive formerPie according to intermTable
        else if(keyPredSet.contains(earlyPie)){
            joinedPie=earlyPie;
            unJoinedPie=laterPie;
            beforeIncreDeriveFromIntermJoin(ParentNode,joinedPie ,unJoinedPie,earlyPie,laterPie,intermNewTab,intermOldTab, leafNode.getBefCol().getNewIEPTable(),leafNode.getBefCol().getIEPTable() );
        }
        // 2. JoinOn latterPie [ first join on latterPie, then derive formerPie according to formalIEList
        else if (keyPredSet.contains(laterPie) ){
            joinedPie=laterPie;
            unJoinedPie=earlyPie;
            LinkList<IE> earlyList=leafNode.formerIEList;
            beforeIncreDeriveFromIEListJoin(ParentNode,joinedPie ,unJoinedPie,earlyPie,laterPie,intermNewTab,intermOldTab, leafNode.getBefCol().getNewIEPTable(),leafNode.getBefCol().getIEPTable(),earlyList );
        }
        else{
            throw new IllegalStateException("Unexpected state: No matching condition for join logic.");
        }
    }

    private  void mergeIntermedNodeDerivingAfter(TreeNode ParentNode,TreeNode leafNode,Set<EBA> keyPredSet,EBA earlyPie, EBA laterPie, Table  intermNewTab, Table intermOldTab){

        EBA joinedPie;
        EBA unJoinedPie;

        // 3. JoinOn Both  [ first join on latterPie, then derive formerPie according to formalIEList and intermTable
        if (keyPredSet.contains(earlyPie) &&  keyPredSet.contains(laterPie) ){
            increDeriveFromIntermAndIEListJoin(ParentNode,earlyPie,laterPie,intermNewTab,intermOldTab, leafNode.getBefCol().getNewIEPTable(),leafNode.getBefCol().getIEPTable() );
        }
        // 1. JoinOn formerPie [ first traverse intermTable, then derive formerPie according to intermTable
        else if(keyPredSet.contains(earlyPie)){
            joinedPie=earlyPie;
            unJoinedPie=laterPie;
            afterIncreDeriveFromIntermJoin(ParentNode,joinedPie ,unJoinedPie,earlyPie,laterPie,intermNewTab,intermOldTab, leafNode.getAftCol().getNewIEPTable(),leafNode.getAftCol().getIEPTable() );
        }
        // 2. JoinOn latterPie [ first join on latterPie, then derive formerPie according to formalIEList
        else if (keyPredSet.contains(laterPie) ){
            joinedPie=laterPie;
            unJoinedPie=earlyPie;
            LinkList<IE> earlyList=leafNode.latterIEList;
            afterIncreDeriveFromIEListJoin(ParentNode,joinedPie ,unJoinedPie,earlyPie,laterPie,intermNewTab,intermOldTab, leafNode.getAftCol().getNewIEPTable(),leafNode.getAftCol().getIEPTable(),earlyList );
        }
        else{
            throw new IllegalStateException("Unexpected state: No matching condition for join logic.");
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
    private void afterIncreDeriveFromIEListJoin(TreeNode ParentNode,EBA joinedPie,EBA unJoinedPie,EBA earlyPie,EBA laterPie,Table intermNewTab, Table intermOldTab,Table leafNewTab,Table leafOldTab,LinkList<IE> earlyIEList){
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
    private void beforeIncreDeriveFromIEListJoin(TreeNode ParentNode,EBA joinedPie,EBA unJoinedPie,EBA earlyPie,EBA laterPie,Table intermNewTab, Table intermOldTab,Table leafNewTab,Table leafOldTab,LinkList<IE> earlyIEList){
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


    private void afterIncreDeriveFromIntermJoin(TreeNode ParentNode,EBA joinedPie,EBA unJoinedPie,EBA earlyPie,EBA laterPie,  Table  intermNewTab, Table intermOldTab,Table leafNewTab,Table leafOldTab ){
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

    private void beforeIncreDeriveFromIntermJoin(TreeNode ParentNode,EBA joinedPie,EBA unJoinedPie,EBA earlyPie,EBA laterPie,  Table  intermNewTab, Table intermOldTab,Table leafNewTab,Table leafOldTab ){
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


}
