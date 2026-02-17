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

## Distributed Batching (Centralized Ingestion)

When multiple independent workers need to write to the same Delta table but cannot coordinate with each other, the ultimate solution is a **Centralized Ingestion Service**.

### The Pattern
1. **Central Service (FastAPI)**: A single service that receives records from all workers and buffers them in memory.
2. **Background Flush**: The service periodically flushes its buffer (e.g., every 5 seconds) to Delta Lake in a single large transaction.
3. **Independent Workers**: They simply send their data to the service via HTTP and don't need to worry about Delta Log concurrency or retries.

### Benchmark Results (10 Distributed Workers)
| Scenario | Total Rows | Effective Throughput | Concurrency Issues |
| :--- | :--- | :--- | :--- |
| Direct Write | 1000 | ~10 writes/s | **High (Many Retries)** |
| **Centralized Ingestion** | **1000** | **~1,100 writes/s** | **Zero (1 Commit)** |

### Why this works
- **Serializes the Log**: It converts the "thundering herd" of writers into a single, predictable writer process.
- **De-couples Workers**: Workers are no longer blocked by Delta Lake's optimistic concurrency checks. They only pay the latency of a quick HTTP request.
- **Extreme Speed**: Because the service batches thousands of rows per commit, it achieves the massive throughput of the "Batched" scenario even with hundreds of distributed producers.

## Reliability & Fault Tolerance

In the final evolution of the ingestion service, we moved from an in-memory buffer to a **Persistent Write-Ahead Log (WAL)** using SQLite.

### The Reliable Ingestion Pattern
1. **Persistent Accept**: Every record is immediately written to a local SQLite database before the HTTP 200 response is sent.
2. **Atomic Flush**:
    - The flusher reads rows from SQLite.
    - It commits them to Delta Lake as a batch.
    - **Crucially**, it only deletes the rows from SQLite *after* the Delta Lake commit is confirmed.
3. **Crash Recovery**: If the service dies, the pending records stay in the SQLite database. On restart, the background worker automatically finds and flushes them.

### Verification (Crash Test)
- We sent **500 records** to the service and **killed the process** before the flush interval.
- **SQLite Check**: Confirmed 500 records remained on disk.
- **Restart Check**: Upon restart, the service immediately picked up the 500 rows and successfully committed them to Delta Lake.

## Final Summary of Scaling Strategies

| Strategy | Ideal Use Case | Pros | Cons |
| :--- | :--- | :--- | :--- |
| **Batching** | Single Application | 100x+ Throughput | Needs manual buffering |
| **Centralized Ingestion** | Distributed Workers | Scalable, Handles "Thundering Herd" | Single point of entry |
| **SQLite WAL** | Mission-Critical Data | Fault-Tolerant, Crash-Proof | Slight latency for DB write |

### Conclusion
To scale Delta Lake writes beyond the physical limits of the log: **Batch your writes**. To make it reliable in a distributed environment: **Use a fault-tolerant centralized service with a disk-backed buffer.**

## How to Reproduce
Run the orchestrator script:
```bash
uv run python orchestrator.py --concurrency 100
```
Each writer will target a unique partition `p_0`, `p_1`, etc.
