## PieStream

**PieStream** implements a method for efficient interval temporal pattern detection in complex event processing (CEP) for data streaming. 

Traditional CEP systems struggle with handling complex queries due to the inefficiency of sequential pattern matching. Our approach casts binary event pairs with interval relationships into regular expressions and automata, enabling low-latency pattern recognition. By decomposing complex patterns into independent event pairs and using natural joins, we achieve more accurate and efficient detection results. 

Experimental evaluations show that our method significantly outperforms state-of-the-art techniques, offering nearly quasilinear time and space complexity.

### Challenges

1. **Event Latency Issues**
   Traditional methods require waiting for complete event intervals before detection, resulting in high latency that fails to meet real-time streaming requirements. Approaches like TPStream exhibit high average processing latency under extreme data input rates.

2. **Inefficient Complex Query Processing**
   Complex queries (e.g., combinations of interval event pairs) lead to exponential time complexity. Existing methods (e.g., TPStream) suffer severe performance degradation as problem scale increases, becoming unsuitable for large-scale streaming data.

3. **Imprecise Temporal Relationship Semantics**
   Traditional Allen relations (e.g., before/after) generate high false positive rates, wasting system resources and reducing detection accuracy.

---

### Solution Approach

1. **Atomic Low-Latency Detection**
   Encode single-temporal-relation interval event pairs into symbolic regular expressions and automata, enabling event-level immediate detection to eliminate waiting latency.

2. **Divide-and-Conquer Optimization**
   Employ a two-phase decomposition:
- Split complex patterns into atomic event pairs for independent detection
- Merge results via incremental natural joins (materializing intermediate results within windows reduces redundant computations)
  Time complexity optimized from exponential to quasi-linear.

3. **Precise Temporal Relationship Modeling**
   Introduce new relational operators:
- `followed-by` (immediately succeeded by)
- `follows` (immediately preceded by)
  Replacing traditional before/after relations, reducing false positives from 79% to 0% in LNG cases.

---

### Key Contributions

1. **Low-Latency Detection Framework**
   Pioneered an automata-based instant detection mechanism for atomic event pairs, enabling stream-level event processing with >30% latency reduction under extreme throughput.

2. **Quasi-Linear Complexity Algorithm**
   Breakthrough in complex query performance via divide-and-conquer and incremental joins:
- Time complexity: exponential → quasi-linear
- Space complexity: quasi-linear

3. **Zero False-Positive Temporal Model**
   Defined precise temporal semantic operators that: Completely eliminate semantic ambiguity in traditional before/after relations 

### Experiments

#### Processing Time

The experiment aimed to compare the processing time of **PieStream** and **[TPStream](http://uni-marburg.de/oaCPk)** using the same synthetic dataset of TPStream and query statements. The query complexity was varied by adjusting the number of PIEs, with each sequence of n PIEs forming n - 1 mPiePairs, each containing 6 Allen temporal relations.

The experiment used a window size of 10,000 and the fastest event input rate, testing different event counts (ranging from 10³ to 10⁷). Multiple trials were conducted, varying the number of PIEs from 4 to 24, to evaluate processing time and system scalability.


<div style="display: flex; justify-content: space-around;">
    <img src="attachments/pies_time.png" alt="pies_time" style="width: 48%;"/>
    <img src="attachments/events_time.png" alt="events_time" style="width: 48%;"/>
</div>

The left figure shows that PieStream’s processing time increases quasilinearly with problem size, while TPStream grows exponentially. TPStream fails when the number of PIEs exceeds 16 due to its direct matching approach. PieStream, though slower for small problems due to overhead from index construction, outperforms TPStream as the problem size increases by reducing redundant computations through indexing and result management.

The right figure shows that PieStream’s processing time remains lower than TPStream once the number of PIEs exceeds 12. While TPStream performs better for small sizes, the performance gap widens as the problem size grows. 

Note: Experiments of PieStream are all included in `scripts` folder.

### Conclusion

We propose a novel framework for efficient complex event processing (CEP) that casts binary event pairs with interval relationships into regular expressions and automata, enabling low-latency recognition. By decomposing complex patterns into independent event pairs and using natural joins, we achieve more accurate and efficient detection. Experimental results show that our approach outperforms state-of-the-art methods in handling complex queries.


## How to Run the Code

1. Clone the repository:

    ```bash 
    cd PieStream
    ```

2. Compile the source code:

    ```bash
    mvn clean install
    ```

3. Modify `scripts/env` and run the experiment scripts in `scripts` folder.
   
    ```bash
    cd scripts
    # firstly modify scripts/env 
    ./processedTime.sh
    ```
4. Review the results in the `scripts/out` directory.

## Acknowledgements

This work is supported by Projects of International Cooperation and Exchanges NSFC-DFG (Grant No. 62061136006).

