package org.piestream.merger;

import java.util.*;

public class MapMerger {

    public static long merge_simple = 0;  // Time taken for simple merge operations
    public static long startTime = 0;     // Start time for merge operations
    public static long endTime = 0;       // End time for merge operations

    // Static generic method to merge two nested maps of type Map<A, Map<B, List<T>>>
    public static <A, B, T> void mergeNestedMaps(Map<A, Map<B, List<T>>> indexMap, Map<A, Map<B, List<T>>> updateindexMap) {
        // Iterate over each entry in the updateindexMap
        for (Map.Entry<A, Map<B, List<T>>> updateEntry : updateindexMap.entrySet()) {
            A keyA = updateEntry.getKey();                    // Extract the key of type A
            Map<B, List<T>> updateTimeMap = updateEntry.getValue();  // Extract the map for keyA

            // If the keyA does not exist in indexMap, initialize it
            if (!indexMap.containsKey(keyA)) {
                indexMap.put(keyA, new HashMap<>());
            }

            // Get the corresponding map for keyA from indexMap
            Map<B, List<T>> indexTimeMap = indexMap.get(keyA);

            // Iterate over each entry in the nested map (keyB -> List<T>) of updateTimeMap
            for (Map.Entry<B, List<T>> updateTimeEntry : updateTimeMap.entrySet()) {
                B keyB = updateTimeEntry.getKey();  // Extract the key of type B
                List<T> updateRows = updateTimeEntry.getValue();  // Extract the list of rows for keyB

                // If keyB does not exist in indexTimeMap, initialize it with an empty list
                if (!indexTimeMap.containsKey(keyB)) {
                    indexTimeMap.put(keyB, new ArrayList<>());
                }

                // Add all update rows to the corresponding list in indexTimeMap
                indexTimeMap.get(keyB).addAll(updateRows);
            }
        }
    }

    // Static generic method to merge two simple maps of type Map<B, List<T>>
    public static <B, T> void mergeSimpleMaps(Map<B, List<T>> indexMap, Map<B, List<T>> updateindexMap) {
        startTime = System.currentTimeMillis();  // Record the start time of the merge

        // Iterate over each entry in the updateindexMap
        for (Map.Entry<B, List<T>> updateEntry : updateindexMap.entrySet()) {
            B keyB = updateEntry.getKey();              // Extract the key of type B
            List<T> updateRows = updateEntry.getValue(); // Extract the list of rows for keyB

            // If the keyB does not exist in indexMap, initialize it with an empty list
            if (!indexMap.containsKey(keyB)) {
                indexMap.put(keyB, new ArrayList<>());
            }

            // Add all update rows to the corresponding list in indexMap
            indexMap.get(keyB).addAll(updateRows);
        }

        endTime = System.currentTimeMillis();  // Record the end time of the merge
        merge_simple += (endTime - startTime);  // Calculate and accumulate the time taken for the merge
    }

    public static void main(String[] args) {
        // Example usage: Merging two nested maps of type Map<A, Map<B, List<T>>>
        Map<String, Map<Long, List<String>>> indexMap = new HashMap<>();
        Map<String, Map<Long, List<String>>> updateindexMap = new HashMap<>();

        Map<Long, List<String>> innerMap1 = new HashMap<>();
        innerMap1.put(1L, new ArrayList<>(Arrays.asList("Row1", "Row2")));  // Add data to innerMap1
        updateindexMap.put("Key1", innerMap1);  // Put innerMap1 into the updateindexMap

        Map<Long, List<String>> innerMap2 = new HashMap<>();
        innerMap2.put(1L, new ArrayList<>(Arrays.asList("Row3", "Row4")));  // Add data to innerMap2
        indexMap.put("Key1", innerMap2);  // Put innerMap2 into the indexMap

        // Merge the two maps using the mergeNestedMaps method
        MapMerger.mergeNestedMaps(indexMap, updateindexMap);

        // Example usage: Merging two simple maps of type Map<B, List<T>>
        Map<Long, List<String>> simpleIndexMap = new HashMap<>();
        Map<Long, List<String>> simpleUpdateMap = new HashMap<>();

        simpleUpdateMap.put(1L, new ArrayList<>(Arrays.asList("Row5", "Row6")));  // Add data to simpleUpdateMap
        simpleIndexMap.put(1L, new ArrayList<>(Arrays.asList("Row7")));  // Add data to simpleIndexMap

        // Merge the two simple maps using the mergeSimpleMaps method
        MapMerger.mergeSimpleMaps(simpleIndexMap, simpleUpdateMap);
    }
}
