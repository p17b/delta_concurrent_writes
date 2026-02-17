# Concurrent Writes Test Results (with Retries)

We upgraded the tester with **exponential backoff retry logic** and scaled up to 1000 parallel threads.

## Performance Summary

| Concurrent Writers | Success Rate | Max Retries | Max Wait Time | Total Duration |
| :--- | :--- | :--- | :--- | :--- |
| 10 | 100% | 0 | 0s | 0.5s |
| 100 | 100% | 0 | 0s | 13.7s |
| 1000 | 100% | 10 | ~350s | ~670s (11m) |
| 2000+ | - | - | - | **Deadlocked/Extreme Contention** |

## Key Findings

### 1. Scaling Bottleneck: The Delta Log
Even though writers target different partitions, all writers must synchronize on the `_delta_log/` to commit their transactions. Delta Lake uses optimistic concurrency control which requires creating a new JSON file for each commit.
- At 1000 concurrent writers, we saw some tasks successfully commit only after **10 retry attempts**.
- Total wait time for some writers reached **over 5 minutes** just to secure a commit.

### 2. Retries are Essential
Without retries, a 1000-concurrency test results in ~2% failure rate. With exponential backoff, we achieved **100% success** at 1000 concurrency, but at the cost of high latency for the last few writers.

### 3. Local Hardware Limits
Attempting 2000 concurrent writes resulted in extreme system contention. Managing 2000 active threads all fighting for filesystem locks and IO resulted in no progress for several minutes, indicating we hit the practical limit for a single machine.

## Optimized Throughput Results (Append-Only & No-Merge)

We applied the suggested optimizations: removed `schema_mode="merge"` and set `delta.appendOnly = true`.

| Setup | Concurrency | Writes/Second | Max Retries | Peak Wait |
| :--- | :--- | :--- | :--- | :--- |
| Baseline | 1 | 5.64 | 0 | 0s |
| **Optimized** | **1** | **10.20** | **0** | **0s** |
| Baseline | 50 | 9.93 | 0 | 0s |
| **Optimized** | **50** | **3.57** | **8** | **77.8s** |

### The Counter-Intuitive Finding
**Faster individual writes = Slower concurrent throughput.**
- **Sequential Success**: By removing the schema-merge overhead, sequential throughput nearly **doubled** (from 5.6 to 10.2 writes/s).
- **Concurrent Failure**: At 50 workers, the optimized writers finished their "prep work" so fast that they hit the `_delta_log` almost simultaneously. This caused a massive spike in transaction conflicts.
- **The Result**: Workers spent more time in exponential backoff (up to 77 seconds) than actually writing, dragging the effective throughput down to 3.57 writes/s.

## Final Recommendations for Scaling Delta Writes

1.  **Batching is King**: Neither append-only nor removing merges can overcome the serial bottleneck of the Delta Log. You **must** group rows into larger batches (e.g., 1000+ rows per commit) to achieve high throughput.
2.  **Concurrency Limit**: For unbatched writes on a local filesystem, keep concurrency low (10-20 workers). Higher concurrency just leads to "thrashing" and retries.
3.  **Schema Enforcement**: Only use `schema_mode="merge"` when necessary. It adds a non-trivial overhead to every transaction commit.
4.  **AppendOnly**: Use `delta.appendOnly` when your use case allows. It clarifies intent and simplifies the metadata tracking, although it won't magically solve the log contention issue.

### Conclusion
Delta Lake is an incredibly robust storage layer for concurrent data ingestion, but it is **not** optimized for high-frequency small transactions. Its performance shines when you aggregate many records into a single, high-throughput batch.

## How to Reproduce
Run the orchestrator script:
```bash
uv run python orchestrator.py --concurrency 100
```
Each writer will target a unique partition `p_0`, `p_1`, etc.
