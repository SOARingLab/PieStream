package org.piestream.engine;

import org.piestream.merger.TreeNode;
import org.piestream.parser.MPIEPairSource;
import org.piestream.piepair.PIEPair;
import org.piestream.events.PointEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The MPIEPairsManager class is responsible for managing multiple MPIEPairs within the event stream processing system.
 * It handles the initialization and management of PIEPairs associated with each MPIEPairSource,
 * and provides functionality for processing events across all PIEPairs in the system.
 *
 * It performs the following main tasks:
 * 1. Initializes MPIEPairs from MPIEPairSource data and stores them in appropriate collections.
 * 2. Maintains mappings between MPIEPairSource objects and their corresponding MPIEPairs.
 * 3. Provides access to MPIEPair objects and their associated PIEPairs.
 * 4. Executes the event processing (stepByPE method) across all MPIEPairs for a given PointEvent.
 */
public class MPIEPairsManager {

    private final List<MPIEPairSource> MPPSourceList;  // List of MPIEPair sources
    private final List<MPIEPair> MPIEPairList;        // List of MPIEPairs
    private final Map<MPIEPairSource, MPIEPair> MPPSourceToPairMap;  // Mapping from MPIEPairSource to MPIEPair
    private final List<PIEPair> AllPiePairs;         // List of all PIEPairs from all MPIEPairs
    private Map<MPIEPairSource, TreeNode> source2Node = new HashMap<>();  // Mapping from MPIEPairSource to TreeNode

    /**
     * Constructor to initialize the MPIEPairsManager with a list of MPIEPairSource objects and a mapping
     * from MPIEPairSource to TreeNode. It initializes the MPIEPairs and stores them in various internal structures.
     *
     * @param MPPSourceList List of MPIEPairSource objects that define the sources of the MPIEPairs
     * @param source2Node Mapping from MPIEPairSource to TreeNode that corresponds to each source
     */
    public MPIEPairsManager(List<MPIEPairSource> MPPSourceList, Map<MPIEPairSource, TreeNode> source2Node) {
        this.MPPSourceList = MPPSourceList;
        this.MPIEPairList = new ArrayList<>();
        this.MPPSourceToPairMap = new HashMap<>();
        this.source2Node = source2Node;
        this.AllPiePairs = new ArrayList<>();

        // Initialize each MPIEPairSource and its corresponding MPIEPair,
        // adding them to the lists and mappings.
        for (MPIEPairSource MPPSource : MPPSourceList) {
            MPIEPair mpiePair = new MPIEPair(MPPSource.getOriginRelations(), MPPSource.getFormerPred(),
                    MPPSource.getLatterPred(), source2Node.get(MPPSource));
            this.MPIEPairList.add(mpiePair);
            this.MPPSourceToPairMap.put(MPPSource, mpiePair);  // Mapping MPPSource to its corresponding MPIEPair
            this.AllPiePairs.addAll(mpiePair.getPiePairs());  // Add all PIEPairs from the current MPIEPair to the list
        }
    }

    /**
     * Returns the list of MPIEPairSource objects managed by this manager.
     *
     * @return List of MPIEPairSource objects
     */
    public List<MPIEPairSource> getMPPSourceList() {
        return MPPSourceList;
    }

    /**
     * Returns the list of MPIEPairs managed by this manager.
     *
     * @return List of MPIEPair objects
     */
    public List<MPIEPair> getMPIEPairList() {
        return MPIEPairList;
    }

    /**
     * Returns the mapping of MPIEPairSource to MPIEPair.
     *
     * @return Map of MPIEPairSource to MPIEPair
     */
    public Map<MPIEPairSource, MPIEPair> getMPPSourceToPairMap() {
        return MPPSourceToPairMap;
    }

    /**
     * Returns the list of all PIEPair objects from all MPIEPairs managed by this manager.
     *
     * @return List of PIEPair objects
     */
    public List<PIEPair> getAllPiePairs() {
        return AllPiePairs;
    }

    /**
     * Retrieves the corresponding MPIEPair for a given MPIEPairSource.
     *
     * @param source The MPIEPairSource for which the corresponding MPIEPair is requested
     * @return The MPIEPair associated with the given MPIEPairSource
     */
    public MPIEPair getMPIEPairBySource(MPIEPairSource source) {
        return MPPSourceToPairMap.get(source);
    }

    /**
     * Executes the stepByPE method of each MPIEPair for a given PointEvent.
     * This method processes the incoming event across all PIEPairs.
     *
     * @param event The PointEvent to be processed by each MPIEPair
     */
    public void runByPE(PointEvent event) {
        for (MPIEPair mpp : MPIEPairList) {
            mpp.run(event);  // Sequentially executes stepByPE for each MPIEPair
        }
    }
}
