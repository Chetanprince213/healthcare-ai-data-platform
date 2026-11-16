# PySpark Join Optimization — Advanced PySpark Optimization Evidence

## Objective
Compare standard joins with broadcast joins and inspect Spark physical execution plans.

## Implementation
- Script: `src/spark/join_optimization.py`
- Engine: Apache Spark using PySpark
- Execution: Local Spark cluster (`local[2]`)
- Driver memory: 2 GB
- Shuffle partitions: 2
- Dataset: Synthetic healthcare records

## Techniques demonstrated
1. Standard join and shuffle execution plan
2. Broadcast join using `broadcast()`
3. `SortMergeJoin` versus `BroadcastHashJoin`
4. `Exchange` and `BroadcastExchange`
5. Spark physical execution plan inspection using `explain(True)`

## Verification
- Optimized physical plan contains `BroadcastHashJoin`.
- Optimized join returns five patient records.
- Script completed with `PYSPARK JOIN OPTIMIZATION VERIFIED SUCCESSFULLY`.

## Result
PASS — Real local PySpark execution verified.

## Limitations
This exercise demonstrates join-plan optimization, not a production performance benchmark. Broadcast joins are appropriate when the broadcast input is sufficiently small. No cloud Spark cluster was deployed.