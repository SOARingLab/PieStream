package org.example.utils;

import java.util.*;

public class MapMerger {

    // 静态泛型方法来合并两个 Map<A, Map<B, List<T>>>
    public static <A, B, T> void mergeNestedMaps(Map<A, Map<B, List<T>>> indexMap, Map<A, Map<B, List<T>>> updateindexMap) {
        for (Map.Entry<A, Map<B, List<T>>> updateEntry : updateindexMap.entrySet()) {
            A keyA = updateEntry.getKey();
            Map<B, List<T>> updateTimeMap = updateEntry.getValue();

            if (!indexMap.containsKey(keyA)) {
                indexMap.put(keyA, new HashMap<>());
            }

            Map<B, List<T>> indexTimeMap = indexMap.get(keyA);

            for (Map.Entry<B, List<T>> updateTimeEntry : updateTimeMap.entrySet()) {
                B keyB = updateTimeEntry.getKey();
                List<T> updateRows = updateTimeEntry.getValue();

                if (!indexTimeMap.containsKey(keyB)) {
                    indexTimeMap.put(keyB, new ArrayList<>());
                }

                indexTimeMap.get(keyB).addAll(updateRows);
            }
        }
    }

    // 静态泛型方法来合并两个 Map<B, List<T>>
    public static <B, T> void mergeSimpleMaps(Map<B, List<T>> indexMap, Map<B, List<T>> updateindexMap) {
        // 遍历 updateindexMap 中的每个 entry
        for (Map.Entry<B, List<T>> updateEntry : updateindexMap.entrySet()) {
            B keyB = updateEntry.getKey();  // 获取 B 键
            List<T> updateRows = updateEntry.getValue();  // 获取对应的 List<T>

            // 如果 indexMap 中没有该 B，则直接放入新的条目
            if (!indexMap.containsKey(keyB)) {
                indexMap.put(keyB, new ArrayList<>());
            }

            // 将 updateindexMap 中的 List<T> 加入到 indexMap 的对应条目中
            indexMap.get(keyB).addAll(updateRows);
        }
    }

    public static void main(String[] args) {
        // 示例用法：合并两层 Map<A, Map<B, List<T>>>
        Map<String, Map<Long, List<String>>> indexMap = new HashMap<>();
        Map<String, Map<Long, List<String>>> updateindexMap = new HashMap<>();

        Map<Long, List<String>> innerMap1 = new HashMap<>();
        innerMap1.put(1L, new ArrayList<>(Arrays.asList("Row1", "Row2")));
        updateindexMap.put("Key1", innerMap1);

        Map<Long, List<String>> innerMap2 = new HashMap<>();
        innerMap2.put(1L, new ArrayList<>(Arrays.asList("Row3", "Row4")));
        indexMap.put("Key1", innerMap2);

        MapMerger.mergeNestedMaps(indexMap, updateindexMap);
        System.out.println(indexMap);

        // 示例用法：合并单层 Map<B, List<T>>
        Map<Long, List<String>> simpleIndexMap = new HashMap<>();
        Map<Long, List<String>> simpleUpdateMap = new HashMap<>();

        simpleUpdateMap.put(1L, new ArrayList<>(Arrays.asList("Row5", "Row6")));
        simpleIndexMap.put(1L, new ArrayList<>(Arrays.asList("Row7")));

        MapMerger.mergeSimpleMaps(simpleIndexMap, simpleUpdateMap);
        System.out.println(simpleIndexMap);
    }
}
