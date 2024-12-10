
# Interval Temporal Pattern Matching in Data Streaming

An open-source implementation to Detect Interval Temporal Patterns in Data Streaming.


## Supplement to the Paper

Due to space limitations in the paper, some content could not be elaborated in detail. This section provides additional information:

### Proofs  


#### Proof of Theorem 1 (Appendix B)

We prove the two directions of the equivalence.

**Sufficiency:**  
Assume that a result $\mathcal{Y}$ satisfies the Pattern query. For each mPiePair $C_i(pie_{\varphi_i}, pie_{\psi_i})$ in the Pattern query, there must exist a pair $(ie_j, ie_k)$ such that $C(ie_j, ie_k)$ holds.  
Since the interval events in $\mathcal{Y}$ correspond to unique PIEs, the result set $\mathcal{Y}$ consists of distinct event pairs. Therefore, after performing the natural join on these mPiePairs, the resulting set will be unique and consistent, and the final result will still satisfy the detection result of the mPiePairs followed by natural joins.

**Necessity:**  
Now assume that a result $\mathcal{Y}$ is obtained by detecting mPiePairs and performing the natural join on these results.  
For each mPiePair $C_i(pie_{\varphi_i}, pie_{\psi_i})$ in the Pattern query, there must exist a pair $(ie_j, ie_k)$ such that $C(ie_j, ie_k)$ holds.  
The natural join operation ensures that only compatible event pairs are combined, so the final result $\mathcal{Y}$ will satisfy the conditions of the Pattern query.

Thus, we have shown that the detection result of a Pattern query is equivalent to the result of detecting mPiePairs followed by natural joins.   ■

#### Proof of Proposition 1 (Appendix A)

...


### Experiments

More experiments are currently in progress.

## Installation and Usage

1. Clone the repository:
   ```bash
   git clone git@github.com:cyann7/PieStream.git
   cd PieStream
    ```

2. Install dependencies:

    ```bash
    mvn clean install
    ```

3. Run the scripts in `scripts` folder.


Experiments and Evaluation codes are included in `scripts` folder.

For bug reports or feature requests, please use the Issues section.
