package org.example.utils;

import org.example.parser.MPIEPairSource;
import org.example.piepair.IEP;
import org.example.piepair.eba.EBA;

import java.util.Set;

public class TreeNode {
    final Set<EBA> predSet;      // Set of predicates associated with the node
    final Set<EBA> keyPredSet;   // Key predicates shared between nodes
    TreeNode parent;       // Parent node in the tree
    TreeNode brother;      // Sibling node, used during merging
    TreeNode left;         // Left child in the tree
    TreeNode right;        // Right child in the tree
    final MPIEPairSource source; // Source object from which the node is created
    int height;            // Height of the node in the tree
    //        IEPQ Q;                // Shared queue
    final IEPCol Col;
//    final IEPTable T;            // IEP table
    Table T;
    final boolean isLeaf;        // Whether this node is a leaf
//    boolean isTrigger;
    //        final IEPUpdateStruct M;     // Update structure

    // Constructor for TreeNode
    public TreeNode(Set<EBA> predSet, Set<EBA> keyPredSet, TreeNode left, TreeNode right,
                    TreeNode parent, TreeNode brother, MPIEPairSource source, int height, boolean isLeaf,Table T,IEPCol Col) {
        this.predSet = predSet;
        this.keyPredSet = keyPredSet;
        this.left = left;
        this.right = right;
        this.parent = parent;
        this.brother = brother;
        this.source = source;
        this.height = height;
        this.Col = Col;
        this.isLeaf = isLeaf;
//        this.isTrigger = false;
        this.T= T;
    }
//
//    public void setTrigger(){
//        isTrigger=true;
//    }
//
//    public void resetTrigger(){
//        isTrigger=false;
//    }
//    public boolean getTrigger(){
//        return isTrigger ;
//    }

    public IEPCol  getCol() {
        return this.Col;
    }

    public TreeNode getParent() {
        return this.parent;
    }

    public TreeNode getBrother() {
        return this.brother;
    }
    public Table getTable(){
        return T;
    }
}