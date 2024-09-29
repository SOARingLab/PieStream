package org.example.engine;

import org.example.merger.TreeNode;
import org.example.parser.MPIEPairSource;
import org.example.piepair.PIEPair;
import org.example.merger.IEPCol;
import org.example.events.PointEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MPIEPairsManager {

    private final List<MPIEPairSource> MPPSourceList;  // MPIEPair 源列表
    private final List<MPIEPair> MPIEPairList;  // MPIEPair 列表
    private final Map<MPIEPairSource, MPIEPair> MPPSourceToPairMap;  // MPPSource到MPIEPair的映射
    private final List<PIEPair> AllPiePairs;  // 所有 PIEPair 对象的列表
//    private final Map<MPIEPairSource, IEPCol> source2Col;  // MPIEPairSource 到 IEPCol 的映射
    private Map<MPIEPairSource, TreeNode> source2Node = new HashMap<>();  // 源到节点的映射

    // 构造函数，初始化 MPIEPairsManager
    public MPIEPairsManager(List<MPIEPairSource> MPPSourceList, Map<MPIEPairSource, TreeNode> source2Node) {
        this.MPPSourceList = MPPSourceList;
        this.MPIEPairList = new ArrayList<>();
        this.MPPSourceToPairMap = new HashMap<>();
        this.source2Node = source2Node;
        this.AllPiePairs = new ArrayList<>();

        // 初始化每个 MPIEPairSource 和对应的 MPIEPair，并将其添加到相应的列表和映射中
        for (MPIEPairSource MPPSource : MPPSourceList) {
            MPIEPair mpiePair = new MPIEPair(MPPSource.getOriginRelations(), MPPSource.getFormerPred(), MPPSource.getLatterPred(), source2Node.get(MPPSource));
            this.MPIEPairList.add(mpiePair);
            this.MPPSourceToPairMap.put(MPPSource, mpiePair);  // MPPSource与MPIEPair的映射
            this.AllPiePairs.addAll(mpiePair.getPiePairs());  // 将MPIEPair中的所有PIEPairs加入AllPiePairs
        }
    }

    // 获取 MPPSource 列表
    public List<MPIEPairSource> getMPPSourceList() {
        return MPPSourceList;
    }

    // 获取 MPIEPair 列表
    public List<MPIEPair> getMPIEPairList() {
        return MPIEPairList;
    }

    // 获取 MPPSource 到 MPIEPair 的映射
    public Map<MPIEPairSource, MPIEPair> getMPPSourceToPairMap() {
        return MPPSourceToPairMap;
    }

    // 获取所有的 PIEPair 对象
    public List<PIEPair> getAllPiePairs() {
        return AllPiePairs;
    }

    // 根据 MPIEPairSource 获取对应的 MPIEPair
    public MPIEPair getMPIEPairBySource(MPIEPairSource source) {
        return MPPSourceToPairMap.get(source);
    }

    // 通过 PointEvent 依次执行所有 PIEPair 的 stepByPE 方法
    public void runByPE(PointEvent event) {
        for (MPIEPair mpp : MPIEPairList) {
            mpp.run(event);  // 串行执行
        }
    }

    // 输出 source2Col 中所有 IEPCol 的内容
    public void print() {
        for (Map.Entry<MPIEPairSource, TreeNode> entry : source2Node.entrySet()) {
            MPIEPairSource source = entry.getKey();
            IEPCol col = entry.getValue().getCol();

            System.out.println("MPIEPairSource: " + source);
            System.out.println("CircularQueue Contents:");

            col.print();  // 打印 IEPCol 的内容

            System.out.println("-----------------------------");
        }
    }
}
