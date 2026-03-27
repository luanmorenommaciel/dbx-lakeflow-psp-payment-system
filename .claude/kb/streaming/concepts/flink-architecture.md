# Flink Architecture

> **Purpose**: JobManager/TaskManager, checkpointing, state backends (ForSt), Flink SQL, disaggregated state
> **Version Coverage**: Apache Flink 2.0+ (March 2025)
> **Confidence**: 0.92
> **MCP Validated**: 2026-03-26

## Overview

Apache Flink is a distributed stream processing engine with a master-worker architecture. The **JobManager** coordinates execution, scheduling, and fault tolerance. **TaskManagers** run the actual data processing. Flink achieves exactly-once semantics through the Chandy-Lamport distributed snapshot algorithm (checkpointing), with state stored in configurable backends.

## The Concept

```yaml
# Flink cluster architecture overview
#
# JobManager (master)
#   ├── Dispatcher       — accepts jobs, launches JobMasters
#   ├── ResourceManager  — manages TaskManager slots
#   └── JobMaster        — manages a single job's execution graph
#
# TaskManager (worker) x N
#   ├── Task Slot 1  — runs pipeline of operators (subtasks)
#   ├── Task Slot 2  — isolated memory, shared TCP connections
#   └── State Backend — RocksDB or HashMapStateBackend
#
# Checkpointing (Chandy-Lamport)
#   1. JobManager injects checkpoint barrier into sources
#   2. Barriers flow through the DAG with the data
#   3. On barrier arrival, operator snapshots its state
#   4. Barrier alignment ensures consistency across parallel streams
#   5. Snapshot stored to durable storage (S3, HDFS, GCS)
```

```sql
-- Flink SQL: configure checkpointing and state backend
SET 'execution.checkpointing.interval'      = '60s';
SET 'execution.checkpointing.mode'          = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause'     = '30s';
SET 'state.backend.type'                    = 'rocksdb';
SET 'state.backend.incremental'             = 'true';
SET 'state.checkpoints.dir'                 = 's3://bucket/flink/checkpoints';
SET 'state.savepoints.dir'                  = 's3://bucket/flink/savepoints';
SET 'state.backend.rocksdb.memory.managed'  = 'true';
```

## Quick Reference

| Component | Role | Failure Impact |
|-----------|------|---------------|
| JobManager | Coordination, scheduling, checkpointing | Job restarts from last checkpoint |
| TaskManager | Data processing, state management | Lost slots rescheduled to other TMs |
| Checkpoint | Consistent distributed snapshot | Recovery point — resume without data loss |
| Savepoint | User-triggered checkpoint (portable) | Job upgrades, cluster migration |
| State Backend | Where operator state lives | RocksDB for large state, HashMap for speed |

| State Backend | Best For | State Size | Performance |
|---------------|----------|-----------|-------------|
| HashMapStateBackend | Small state, low latency | < 1 GB per TM | Fastest (heap) |
| EmbeddedRocksDB | Large state, production (Flink 1.x) | Terabytes | Disk-backed, incremental checkpoints |
| **ForSt (Flink 2.0+)** | **Cloud-native, disaggregated state** | **Hundreds of TB** | **Remote DFS, async access, fast rescaling** |

## Common Mistakes

### Wrong

```sql
-- No checkpointing configured — state lost on failure
-- No state TTL — state grows unbounded

-- Missing: SET 'execution.checkpointing.interval' = '60s';

SELECT user_id, COUNT(*) AS total_clicks
FROM clicks
GROUP BY user_id;
-- State for every user_id grows forever!
```

### Correct

```sql
-- Checkpointing enabled + state TTL to bound state size
SET 'execution.checkpointing.interval' = '60s';
SET 'table.exec.state.ttl'            = '86400000';  -- 24h in ms

SELECT user_id, COUNT(*) AS total_clicks
FROM clicks
GROUP BY user_id;
-- State entries expire after 24 hours of no updates
```

## Flink 2.0: Disaggregated State (ForSt Backend)

```yaml
# Flink 2.0: ForSt state backend — remote DFS as primary storage
state.backend.type: forst
table.exec.async-state.enabled: true

# Checkpointing required with disaggregated state
execution.checkpointing.incremental: true
execution.checkpointing.dir: s3://your-bucket/flink-checkpoints
```

### Why Disaggregated State Matters

| Challenge (Flink 1.x) | Solution (Flink 2.0) |
|------------------------|----------------------|
| Local disk constraints in containers | Remote DFS (S3/GCS/HDFS) as primary storage |
| Spiky resource usage from compaction | Offloaded to remote storage |
| Slow rescaling for large state (TB+) | Fast rescaling — state not tied to TaskManagers |
| Heavy checkpoints on local disk | Light, native checkpoints to remote storage |

### Async State API (State V2)

```java
// Flink 2.0: non-blocking state access for higher throughput
DataStream<Alert> alerts = events
    .keyBy(Event::sourceAddress)
    .enableAsyncState()  // enable async state access
    .flatMap(new StateMachineMapper());

// Inside the mapper: use asyncValue() instead of value()
currentState.asyncValue()
    .thenAccept(state -> {
        State nextState = state.transition(evt.type());
        currentState.asyncUpdate(nextState);
    });
```

### Removed APIs in Flink 2.0

| Removed | Replacement |
|---------|-------------|
| `SourceFunction` / `SinkFunction` | Source V2 / Sink V2 |
| Scala DataStream API | Java DataStream API |
| DataSet API | DataStream API or Table API/SQL |
| `TableSchema`, `TableColumn` | `Schema`, `Column`, `DataTypes` |
| `TableSource` / `TableSink` | `DynamicTableSource` / `DynamicTableSink` |

## Related

- [stream-processing-fundamentals](../concepts/stream-processing-fundamentals.md)
- [flink-sql-patterns](../patterns/flink-sql-patterns.md)
