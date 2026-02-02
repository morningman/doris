# TableStream Architecture Redesign for Doris FE Streaming Insert Framework

## Executive Summary

This document presents a comprehensive architecture redesign for the streaming insert job framework in Apache Doris FE. The goal is to introduce **TableStream** as a first-class abstraction for streaming data sources, and **MetadataPersistenceStrategy** for deployment mode abstraction, creating a clean, layered architecture that:

1. Makes adding new data sources trivial (no core framework changes)
2. Eliminates all `isCloudMode()` checks from business logic
3. Maintains all existing functionality with improved testability

**Current State**:
- Monolithic `StreamingInsertJob` (1559 lines) with source-specific logic scattered throughout
- 16+ `isKafkaStreamingJob()` checks
- 4 `isCloudMode()` checks
- Tight coupling between job management, data source specifics, and deployment mode

**Target State**: Layered architecture where:
- Generic job management layer handles lifecycle, scheduling, persistence, transactions
- `TableStream` interface abstracts ALL source-specific behavior
- `MetadataPersistenceStrategy` interface abstracts ALL deployment-mode-specific behavior
- Adding a new source = implementing 6 small, focused classes (no core framework changes)
- Adding a new deployment mode = implementing 1 strategy class

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Core Interfaces - TableStream](#2-core-interfaces---tablestream)
3. [Core Interfaces - MetadataPersistenceStrategy](#3-core-interfaces---metadatapersistencestrategy)
4. [Class Hierarchy](#4-class-hierarchy)
5. [Refactored Core Classes](#5-refactored-core-classes)
6. [Data Flow by Source Type](#6-data-flow-by-source-type)
7. [Extension Guide: Adding a New Source](#7-extension-guide-adding-a-new-source)
8. [Migration Path](#8-migration-path)
9. [Persistence Compatibility](#9-persistence-compatibility)
10. [Key Design Decisions](#10-key-design-decisions)
11. [Files to Modify](#11-files-to-modify)
12. [Verification Plan](#12-verification-plan)
13. [Implementation TODO List](#13-implementation-todo-list)

---

## 1. Architecture Overview

### 1.1 Current Problems

After analyzing the codebase, the following problems are identified:

**Source-Specific Problems:**
1. **StreamingInsertJob is monolithic**: Contains Kafka-specific logic (partition tasks, idle partition restart), S3-specific logic, JDBC-specific logic all mixed together
2. **No unified data source abstraction**: Each source has different task creation strategies, different success callbacks, different SQL rewriting approaches
3. **Tight coupling**: StreamingInsertJob has `if (isKafkaStreamingJob())` checks scattered throughout (16+ occurrences)
4. **Adding new sources requires modifying StreamingInsertJob**: Must add new conditional branches, new task types, new callbacks
5. **SourceOffsetProvider mixes concerns**: Offset tracking, metadata fetching, and SQL rewriting are all in one interface
6. **Two execution paths**: TVF-based (Kafka/S3) vs non-TVF (JDBC multi-table) have very different flows

**CloudMode Problems:**
7. **Scattered deployment mode checks**: 4 locations with `if (isCloudMode())` checks
8. **Different semantic behaviors**: Statistics are accumulated in non-cloud mode but replaced in cloud mode
9. **Recovery source differs**: Edit log in non-cloud vs cloud meta service in cloud mode
10. **Hard to test**: Cloud-specific paths require cloud infrastructure

### 1.2 Current Execution Models by Source

| Source | Parallelism Model | Task Type | Execution Path |
|--------|-------------------|-----------|----------------|
| **Kafka** | Per-partition independent pipelines | StreamingInsertTask | SQL via TVF rewriting |
| **S3** | Single sequential task | StreamingInsertTask | SQL via TVF rewriting |
| **JDBC/Binlog** | Split-based (snapshot + binlog) | StreamingMultiTblTask | gRPC to BE |

### 1.3 Current CloudMode Usages

| File | Line | Method | Purpose |
|------|------|--------|---------|
| `StreamingJobSchedulerTask.java` | 58 | `handlePendingState()` | Recovery on job startup |
| `StreamingInsertJob.java` | 960 | `replayOnUpdated()` | Sync with cloud after properties change |
| `StreamingInsertJob.java` | 993 | `modifyPropertiesInternal()` | Push offset reset to cloud |
| `StreamingInsertJob.java` | 1534 | `cleanup()` | Delete job metadata from cloud |

### 1.4 Proposed Layered Architecture

```
+=====================================================================+
|               LAYER 1: JOB MANAGEMENT (Generic)                      |
|  StreamingInsertJob (slim ~500 lines) -> delegates to:               |
|    - TableStream (source abstraction)                                |
|    - MetadataPersistenceStrategy (deployment mode abstraction)       |
|  Responsibilities:                                                   |
|  - Job lifecycle (PENDING->RUNNING->PAUSED->STOPPED)                 |
|  - Task scheduling coordination                                      |
|  - Transaction callbacks (TxnStateChangeCallback)                    |
|  - Persistence & recovery                                            |
|  - Auto-resume with exponential backoff                              |
+=====================================================================+
                   |                              |
                   | owns/delegates to            | owns/delegates to
                   v                              v
+============================+     +================================+
| LAYER 2a: TABLE STREAM     |     | LAYER 2b: PERSISTENCE STRATEGY |
| (Source Abstraction)       |     | (Deployment Abstraction)       |
|                            |     |                                |
| TableStream<Cursor, Work>  |     | MetadataPersistenceStrategy    |
| - KafkaTableStream         |     | - LocalMetadataPersistence     |
| - S3TableStream            |     | - CloudMetadataPersistence     |
| - JdbcTableStream          |     |                                |
+============================+     +================================+
        |          |          |
        v          v          v
+==============+ +==============+ +==============+
| LAYER 3a:    | | LAYER 3b:    | | LAYER 3c:    |
| TaskPart-    | | StreamCursor | | SqlRewriter  |
| itioner      | | (Position)   | | (Transform)  |
| (Parallelism)|              |                |
| Per-partition|              |                |
| Sequential   |              |                |
| Split-based  |              |                |
+==============+ +==============+ +==============+
                        |
                        v
+=====================================================================+
|               LAYER 4: INFRASTRUCTURE (Generic)                      |
|  Task execution, Transaction handling, Persistence, Cloud support    |
+=====================================================================+
```

---

## 2. Core Interfaces - TableStream

### 2.1 TableStream<C, W> - The Central Source Abstraction

**Location**: `org.apache.doris.job.stream.TableStream`

```java
/**
 * TableStream represents a streaming data source bound to a consumption position.
 *
 * A TableStream is the source-specific "brain" of a streaming job. It knows:
 * - How to discover available work units (partitions, files, splits)
 * - How to track consumption position (cursor/offset)
 * - How to create executable tasks for those work units
 * - How to rewrite SQL for a specific work unit
 *
 * The generic StreamingInsertJob delegates ALL source-specific decisions to
 * its TableStream instance. Adding a new data source means implementing this
 * interface; no modification to StreamingInsertJob is required.
 *
 * Conceptual SQL model:
 *   INSERT INTO target SELECT * FROM TableStream WHERE position BETWEEN X AND Y
 *
 * @param <C> the concrete cursor type (e.g., KafkaCursor, S3Cursor)
 * @param <W> the concrete work unit type (e.g., KafkaPartitionWork, S3BatchWork)
 */
public interface TableStream<C extends StreamCursor, W extends WorkUnit> {

    /** Unique identifier for this stream type (e.g., "kafka", "s3", "mysql_cdc") */
    String getStreamType();

    /**
     * Initialize the stream from job configuration.
     * Called once when the job is first created or restored from persistence.
     */
    void initialize(StreamConfig config) throws StreamException;

    /**
     * Fetch latest metadata from the remote data source.
     * For Kafka: fetch latest offsets for all partitions.
     * For S3: list new files since last cursor position.
     * For JDBC: fetch latest binlog position.
     *
     * This is called periodically by the scheduler.
     */
    void refreshMetadata() throws StreamException;

    /**
     * Get the current consumption cursor (position in the stream).
     * This is the source of truth for "where we are" in the stream.
     */
    C getCursor();

    /**
     * Update the cursor after successful consumption.
     * @param cursor the new cursor position
     */
    void updateCursor(C cursor);

    /**
     * Restore cursor from persisted state (after FE restart).
     * @param persistedState the serialized cursor state
     */
    void restoreCursor(String persistedState) throws StreamException;

    /**
     * Get the task partitioner that determines parallelism strategy.
     * @return the partitioner for this stream type
     */
    TaskPartitioner<C, W> getTaskPartitioner();

    /**
     * Get the SQL rewriter that adapts the base INSERT SQL for a specific work unit.
     * Returns null for non-SQL execution paths (e.g., JDBC CDC multi-table).
     */
    SqlRewriter<W> getSqlRewriter();

    /**
     * Create an executable task for the given work unit.
     * @param workUnit the work unit describing what data to consume
     * @param taskContext shared context (job properties, user identity, etc.)
     * @return the task ready for execution
     */
    AbstractStreamingTask createTask(W workUnit, StreamTaskContext taskContext);

    /**
     * Handle task completion. Called after a task succeeds.
     * The stream updates its internal cursor based on the result.
     * @param workUnit the completed work unit
     * @param result the execution result (rows consumed, etc.)
     */
    void onTaskCompleted(W workUnit, TaskResult result);

    /**
     * Check if there is more data available to consume.
     */
    boolean hasMoreData();

    /**
     * Get display info for SHOW STREAMING JOBS.
     */
    StreamDisplayInfo getDisplayInfo();

    /**
     * Serialize the cursor for persistence (edit log / cloud meta service).
     */
    String serializeCursor();

    /**
     * Perform cleanup when the job is stopped/dropped.
     */
    void cleanup() throws StreamException;

    /**
     * Get the current stream metadata.
     */
    StreamMetadata getMetadata();
}
```

### 2.2 StreamCursor - Position in the Stream

**Location**: `org.apache.doris.job.stream.StreamCursor`

```java
/**
 * StreamCursor represents the current consumption position in a data stream.
 *
 * Unlike the current Offset interface which only knows serialization,
 * StreamCursor is a richer concept that understands comparison and advancement.
 *
 * Each source type provides its own cursor implementation:
 * - KafkaCursor: Map<partitionId, offset>
 * - S3Cursor: last consumed file name
 * - JdbcCursor: split state + binlog position
 */
public interface StreamCursor {

    /** Serialize to JSON for persistence */
    String toJson();

    /** Deserialize from JSON */
    static StreamCursor fromJson(String json, Class<? extends StreamCursor> type);

    /** Whether this cursor represents the initial (empty) state */
    boolean isInitial();

    /** Whether this cursor is valid for consumption */
    boolean isValid();

    /** Human-readable description for SHOW commands */
    String toDisplayString();
}
```

### 2.3 WorkUnit - A Discrete Chunk of Work

**Location**: `org.apache.doris.job.stream.WorkUnit`

```java
/**
 * WorkUnit represents a discrete, bounded chunk of data to be consumed.
 *
 * It is the contract between the TaskPartitioner (which decides WHAT to consume)
 * and the Task (which executes the consumption).
 *
 * Examples:
 * - KafkaPartitionWork: partition=2, offset=[100, 200)
 * - S3BatchWork: files=[file1.csv, file2.csv, file3.csv]
 * - JdbcSnapshotWork: table=users, split_key=id, range=[1000, 2000)
 * - JdbcBinlogWork: starting_offset={file=mysql-bin.001, pos=1234}
 */
public interface WorkUnit {

    /** Unique identifier for this work unit (used as pipeline identity key) */
    String getWorkUnitId();

    /** Serialize for transaction commit attachment */
    String toJson();

    /** Human-readable range description */
    String toDisplayString();
}
```

### 2.4 TaskPartitioner<C, W> - Parallelism Strategy

**Location**: `org.apache.doris.job.stream.TaskPartitioner`

```java
/**
 * TaskPartitioner determines how a stream's available data is divided into
 * work units for parallel or sequential execution.
 *
 * This is the interface that captures the fundamental difference between
 * Kafka (per-partition parallel), S3 (sequential batch), and JDBC (split-based).
 *
 * The partitioner is stateless -- it reads the current cursor and available
 * metadata from the TableStream, and produces work units.
 *
 * @param <C> the cursor type
 * @param <W> the work unit type
 */
public interface TaskPartitioner<C extends StreamCursor, W extends WorkUnit> {

    /**
     * Determine the initial set of work units when the job first starts.
     * For Kafka: one WorkUnit per partition.
     * For S3: one WorkUnit with the first batch of files.
     * For JDBC: one WorkUnit with the first snapshot split (or binlog start).
     *
     * @param cursor the current cursor position
     * @param metadata the latest stream metadata
     * @return list of work units to execute
     */
    List<W> partitionWork(C cursor, StreamMetadata metadata);

    /**
     * Determine the next work unit for a completed work unit.
     * This enables the independent pipeline model (each partition creates its
     * own successor) without the scheduler having to understand partition logic.
     *
     * Returns null/empty if there is no more work for this pipeline.
     *
     * @param completedUnit the work unit that just finished
     * @param result the execution result
     * @param cursor the updated cursor
     * @param metadata the latest stream metadata
     * @return the next work unit, or null if this pipeline should stop
     */
    W nextWorkUnit(W completedUnit, TaskResult result, C cursor, StreamMetadata metadata);

    /**
     * Check for idle pipelines that should be restarted (e.g., Kafka partitions
     * that stopped due to no data but now have new messages).
     *
     * Returns work units for pipelines that should be restarted.
     * Returns empty list for sources that don't support this (S3, JDBC).
     *
     * @param activePipelines currently active pipeline identifiers
     * @param cursor the current cursor
     * @param metadata the latest metadata
     * @return work units for pipelines to restart
     */
    List<W> findRestartableWork(Set<String> activePipelines, C cursor, StreamMetadata metadata);

    /**
     * Get the parallelism model for this partitioner.
     * Used by the job to understand task management semantics.
     */
    ParallelismModel getParallelismModel();
}

/** Enumeration of supported parallelism models */
enum ParallelismModel {
    /** Per-partition independent pipelines (Kafka) */
    PARALLEL_PIPELINES,
    /** Sequential batch processing (S3) */
    SEQUENTIAL_BATCH,
    /** Split-based with phase transitions (JDBC CDC) */
    SPLIT_BASED
}
```

### 2.5 SqlRewriter<W> - SQL Transformation

**Location**: `org.apache.doris.job.stream.SqlRewriter`

```java
/**
 * SqlRewriter transforms the base INSERT INTO ... SELECT FROM source(...)
 * command into a concrete, bounded command for a specific WorkUnit.
 *
 * For Kafka: rewrites kafka() TVF -> catalog.db.table WHERE _partition=X AND _offset BETWEEN Y AND Z
 * For S3: rewrites s3() TVF with updated file list URI
 * For JDBC: returns null (JDBC uses gRPC, not SQL execution)
 *
 * @param <W> the work unit type
 */
public interface SqlRewriter<W extends WorkUnit> {

    /**
     * Rewrite the base INSERT command for a specific work unit.
     *
     * @param baseCommand the parsed base command (from user's CREATE JOB SQL)
     * @param workUnit the work unit defining what data to consume
     * @return the rewritten command ready for execution
     */
    InsertIntoTableCommand rewrite(InsertIntoTableCommand baseCommand, W workUnit);
}
```

### 2.6 StreamMetadata - Remote State Snapshot

**Location**: `org.apache.doris.job.stream.StreamMetadata`

```java
/**
 * StreamMetadata represents the latest available state of a remote data source.
 * This is fetched periodically and used by TaskPartitioner to determine work units.
 *
 * Implementations are source-specific:
 * - KafkaStreamMetadata: Map<partitionId, latestOffset>
 * - S3StreamMetadata: latest file name
 * - JdbcStreamMetadata: latest binlog position
 */
public interface StreamMetadata {
    /** Whether the metadata is fresh enough to make decisions */
    boolean isValid();

    /** When this metadata was last refreshed */
    long getRefreshTimestamp();
}
```

### 2.7 Supporting Classes

```java
/**
 * Shared context passed to task creation.
 */
public class StreamTaskContext {
    private final long jobId;
    private final String executeSql;
    private final String currentDbName;
    private final UserIdentity createUser;
    private final StreamingJobProperties jobProperties;
    // ... getters, constructor
}

/**
 * Result of a task execution, used to update the cursor.
 */
public class TaskResult {
    private final long scannedRows;
    private final long loadBytes;
    private final long fileNumber;
    private final long fileSize;
    private final String offsetJson;  // serialized offset from txn attachment
    // ... getters, constructor
}

/**
 * Display information for SHOW commands.
 */
public class StreamDisplayInfo {
    private final String currentOffset;
    private final String maxOffset;
    private final String statistics;
    // ... getters, constructor
}

/**
 * Configuration for initializing a TableStream.
 */
public class StreamConfig {
    private final Map<String, String> properties;
    private final Map<String, String> tvfProperties;
    private final StreamingJobProperties jobProperties;
    // ... factory methods, getters
}
```

---

## 3. Core Interfaces - MetadataPersistenceStrategy

### 3.1 Design Principle

Introduce a **`MetadataPersistenceStrategy`** interface that encapsulates ALL deployment-mode-specific operations. The streaming job framework will delegate to this strategy without knowing whether it's running in cloud or non-cloud mode.

### 3.2 Key Semantic Differences Between Modes

| Aspect | Non-Cloud Mode | Cloud Mode |
|--------|----------------|------------|
| **Recovery Source** | Edit log replay | Cloud meta service (authoritative) |
| **Statistics Update** | Accumulate (`+= value`) | Replace (`= value`) |
| **Offset Reset** | Local only (edit log persists) | Push to cloud immediately |
| **Cleanup** | Local cleanup only | Delete from both local and cloud |
| **Source of Truth** | Edit log | Cloud meta service |

### 3.3 MetadataPersistenceStrategy Interface

**Location**: `org.apache.doris.job.stream.persistence.MetadataPersistenceStrategy`

```java
/**
 * MetadataPersistenceStrategy abstracts the differences between cloud mode
 * and non-cloud mode for streaming job metadata persistence.
 *
 * In cloud mode:
 * - Cloud meta service is the authoritative source of truth
 * - Statistics are replaced (not accumulated) during recovery
 * - Offset resets are immediately pushed to cloud
 * - Cleanup removes data from both local and cloud
 *
 * In non-cloud mode:
 * - Edit log is the authoritative source of truth
 * - Statistics are accumulated during replay
 * - Offset resets are stored locally (edit log persists)
 * - Cleanup only removes local data
 *
 * This interface eliminates all isCloudMode() checks from the streaming
 * job framework, ensuring consistent code paths across deployment modes.
 */
public interface MetadataPersistenceStrategy {

    /**
     * Recover job state during PENDING state or after FE restart.
     *
     * Cloud mode: Fetches statistics and offset from cloud meta service.
     * Non-cloud mode: No-op (edit log replay handles recovery).
     *
     * @param job the streaming job to recover
     * @throws JobException if recovery fails (cloud RPC error, etc.)
     */
    void recoverJobState(StreamingInsertJob job) throws JobException;

    /**
     * Update job statistics after task completion.
     *
     * Cloud mode: REPLACE statistics (set absolute values from attachment).
     * Non-cloud mode: ACCUMULATE statistics (add values to existing).
     *
     * @param currentStats the current job statistics (may be null)
     * @param attachment the task completion attachment with new values
     * @return updated statistics
     */
    StreamingJobStatistic updateStatistics(
            StreamingJobStatistic currentStats,
            StreamingTaskTxnCommitAttachment attachment);

    /**
     * Reset job offset (called when user modifies offset via ALTER JOB).
     *
     * Cloud mode: Push new offset to cloud meta service.
     * Non-cloud mode: No-op (edit log persistence handles it).
     *
     * @param dbId database ID
     * @param jobId job ID
     * @param offset the new offset value
     * @throws JobException if reset fails
     */
    void resetOffset(long dbId, long jobId, Offset offset) throws JobException;

    /**
     * Cleanup job metadata when job is stopped/dropped.
     *
     * Cloud mode: Delete from cloud meta service.
     * Non-cloud mode: No-op (only local cleanup needed).
     *
     * @param dbId database ID
     * @param jobId job ID
     * @throws JobException if cleanup fails
     */
    void cleanup(long dbId, long jobId) throws JobException;

    /**
     * Check if recovery should be performed during replay on updated.
     *
     * Cloud mode: Returns true (need to sync from cloud).
     * Non-cloud mode: Returns false (edit log has all data).
     */
    boolean needRecoveryOnReplay();

    /**
     * Get the persistence mode name for logging.
     */
    String getModeName();
}
```

### 3.4 LocalMetadataPersistence (Non-Cloud Mode)

**Location**: `org.apache.doris.job.stream.persistence.LocalMetadataPersistence`

```java
/**
 * Local edit log based persistence strategy (non-cloud mode).
 *
 * - Recovery is handled by edit log replay
 * - Statistics are accumulated
 * - No remote operations needed
 */
public class LocalMetadataPersistence implements MetadataPersistenceStrategy {

    @Override
    public void recoverJobState(StreamingInsertJob job) throws JobException {
        // No-op: Edit log replay handles recovery
        // The job's gsonPostProcess() and replayOffsetProviderIfNeed()
        // restore state from persisted fields
    }

    @Override
    public StreamingJobStatistic updateStatistics(
            StreamingJobStatistic currentStats,
            StreamingTaskTxnCommitAttachment attachment) {

        StreamingJobStatistic stats = currentStats;
        if (stats == null) {
            stats = new StreamingJobStatistic();
        }

        // ACCUMULATE: Add new values to existing
        stats.setScannedRows(stats.getScannedRows() + attachment.getScannedRows());
        stats.setLoadBytes(stats.getLoadBytes() + attachment.getLoadBytes());
        stats.setFileNumber(stats.getFileNumber() + attachment.getNumFiles());
        stats.setFileSize(stats.getFileSize() + attachment.getFileBytes());

        return stats;
    }

    @Override
    public void resetOffset(long dbId, long jobId, Offset offset) throws JobException {
        // No-op: Local offset update is sufficient
        // Edit log persistence (logUpdateOperation) handles durability
    }

    @Override
    public void cleanup(long dbId, long jobId) throws JobException {
        // No-op: Only source-specific cleanup needed (e.g., JDBC chunks)
        // Handled separately by TableStream.cleanup()
    }

    @Override
    public boolean needRecoveryOnReplay() {
        return false;  // Edit log has complete state
    }

    @Override
    public String getModeName() {
        return "local";
    }
}
```

### 3.5 CloudMetadataPersistence (Cloud Mode)

**Location**: `org.apache.doris.job.stream.persistence.CloudMetadataPersistence`

```java
/**
 * Cloud meta service based persistence strategy (cloud mode).
 *
 * - Cloud meta service is the authoritative source of truth
 * - Statistics are replaced (not accumulated)
 * - Offset resets are pushed to cloud immediately
 * - Cleanup removes data from cloud
 */
@Log4j2
public class CloudMetadataPersistence implements MetadataPersistenceStrategy {

    @Override
    public void recoverJobState(StreamingInsertJob job) throws JobException {
        Cloud.GetStreamingTaskCommitAttachRequest.Builder builder =
                Cloud.GetStreamingTaskCommitAttachRequest.newBuilder()
                        .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                        .setCloudUniqueId(Config.cloud_unique_id)
                        .setDbId(job.getDbId())
                        .setJobId(job.getJobId());

        try {
            Cloud.GetStreamingTaskCommitAttachResponse response =
                    MetaServiceProxy.getInstance().getStreamingTaskCommitAttach(builder.build());

            if (response.getStatus().getCode() == Cloud.MetaServiceCode.STREAMING_JOB_PROGRESS_NOT_FOUND) {
                log.info("No cloud progress found for job {}, using local state", job.getJobId());
                return;
            }

            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                throw new JobException(response.getStatus().getMsg());
            }

            // Update job with cloud state
            StreamingTaskTxnCommitAttachment attachment =
                    new StreamingTaskTxnCommitAttachment(response.getCommitAttach());
            job.updateFromCloudRecovery(attachment);

        } catch (RpcException e) {
            log.warn("Failed to recover job {} from cloud", job.getJobId(), e);
            throw new JobException("Cloud recovery failed: " + e.getMessage());
        }
    }

    @Override
    public StreamingJobStatistic updateStatistics(
            StreamingJobStatistic currentStats,
            StreamingTaskTxnCommitAttachment attachment) {

        StreamingJobStatistic stats = currentStats;
        if (stats == null) {
            stats = new StreamingJobStatistic();
        }

        // REPLACE: Set absolute values from cloud (authoritative source)
        stats.setScannedRows(attachment.getScannedRows());
        stats.setLoadBytes(attachment.getLoadBytes());
        stats.setFileNumber(attachment.getNumFiles());
        stats.setFileSize(attachment.getFileBytes());

        return stats;
    }

    @Override
    public void resetOffset(long dbId, long jobId, Offset offset) throws JobException {
        Cloud.ResetStreamingJobOffsetRequest.Builder builder =
                Cloud.ResetStreamingJobOffsetRequest.newBuilder()
                        .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                        .setCloudUniqueId(Config.cloud_unique_id)
                        .setDbId(dbId)
                        .setJobId(jobId)
                        .setOffset(offset.toSerializedJson());

        try {
            Cloud.ResetStreamingJobOffsetResponse response =
                    MetaServiceProxy.getInstance().resetStreamingJobOffset(builder.build());

            if (response.getStatus().getCode() == Cloud.MetaServiceCode.ROUTINE_LOAD_PROGRESS_NOT_FOUND) {
                log.warn("No cloud offset found for job {}, skip reset", jobId);
                return;
            }

            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                throw new JobException(response.getStatus().getMsg());
            }

        } catch (RpcException e) {
            log.warn("Failed to reset cloud offset for job {}", jobId, e);
            throw new JobException("Cloud offset reset failed: " + e.getMessage());
        }
    }

    @Override
    public void cleanup(long dbId, long jobId) throws JobException {
        Cloud.DeleteStreamingJobRequest req = Cloud.DeleteStreamingJobRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id)
                .setDbId(dbId)
                .setJobId(jobId)
                .build();

        try {
            Cloud.DeleteStreamingJobResponse response =
                    MetaServiceProxy.getInstance().deleteStreamingJob(req);

            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                log.warn("Failed to delete job {} from cloud: {}", jobId, response.getStatus());
                // Don't throw - cleanup should be best-effort
            }

        } catch (RpcException e) {
            log.warn("Failed to delete job {} from cloud", jobId, e);
            // Don't throw - cleanup should be best-effort
        }
    }

    @Override
    public boolean needRecoveryOnReplay() {
        return true;  // Need to sync from cloud
    }

    @Override
    public String getModeName() {
        return "cloud";
    }
}
```

### 3.6 MetadataPersistenceFactory

**Location**: `org.apache.doris.job.stream.persistence.MetadataPersistenceFactory`

```java
/**
 * Factory for creating the appropriate persistence strategy.
 */
public class MetadataPersistenceFactory {

    private static volatile MetadataPersistenceStrategy instance;

    /**
     * Get the singleton persistence strategy instance.
     * Determined once at startup based on Config.isCloudMode().
     */
    public static MetadataPersistenceStrategy getInstance() {
        if (instance == null) {
            synchronized (MetadataPersistenceFactory.class) {
                if (instance == null) {
                    instance = Config.isCloudMode()
                            ? new CloudMetadataPersistence()
                            : new LocalMetadataPersistence();
                }
            }
        }
        return instance;
    }

    // For testing
    public static void setInstance(MetadataPersistenceStrategy strategy) {
        instance = strategy;
    }
}
```

---

## 4. Class Hierarchy

### 4.1 Generic Classes (no source-specific or mode-specific logic)

```
org.apache.doris.job.stream/
  TableStream<C, W>           (interface)
  StreamCursor                (interface)
  WorkUnit                    (interface)
  TaskPartitioner<C, W>       (interface)
  SqlRewriter<W>              (interface)
  StreamMetadata              (interface)
  ParallelismModel            (enum)
  StreamConfig                (class)
  StreamTaskContext           (class)
  TaskResult                  (class)
  StreamDisplayInfo           (class)
  StreamException             (class)
  TableStreamFactory          (factory)

org.apache.doris.job.stream.persistence/
  MetadataPersistenceStrategy (interface)
  LocalMetadataPersistence    (non-cloud implementation)
  CloudMetadataPersistence    (cloud implementation)
  MetadataPersistenceFactory  (factory)
```

### 4.2 Source-Specific Classes

#### Kafka
```
org.apache.doris.job.stream.kafka/
  KafkaTableStream            implements TableStream<KafkaCursor, KafkaPartitionWork>
  KafkaCursor                 implements StreamCursor
  KafkaPartitionWork          implements WorkUnit
  KafkaTaskPartitioner        implements TaskPartitioner<KafkaCursor, KafkaPartitionWork>
  KafkaSqlRewriter            implements SqlRewriter<KafkaPartitionWork>
  KafkaStreamMetadata         implements StreamMetadata
```

#### S3
```
org.apache.doris.job.stream.s3/
  S3TableStream               implements TableStream<S3Cursor, S3BatchWork>
  S3Cursor                    implements StreamCursor
  S3BatchWork                 implements WorkUnit
  S3TaskPartitioner           implements TaskPartitioner<S3Cursor, S3BatchWork>
  S3SqlRewriter               implements SqlRewriter<S3BatchWork>
  S3StreamMetadata            implements StreamMetadata
```

#### JDBC/CDC
```
org.apache.doris.job.stream.jdbc/
  JdbcTableStream             implements TableStream<JdbcCursor, JdbcWork>
  JdbcCursor                  implements StreamCursor
  JdbcSnapshotWork            implements WorkUnit
  JdbcBinlogWork              implements WorkUnit
  JdbcTaskPartitioner         implements TaskPartitioner<JdbcCursor, JdbcWork>
  JdbcSqlRewriter             implements SqlRewriter<JdbcWork>  (returns null)
  JdbcStreamMetadata          implements StreamMetadata
```

---

## 5. Refactored Core Classes

### 5.1 StreamingInsertJob (Sketch)

The key transformation is that `StreamingInsertJob` becomes a thin orchestrator that delegates all source-specific decisions to its `TableStream` instance and all deployment-mode-specific decisions to `MetadataPersistenceStrategy`.

```java
/**
 * Refactored StreamingInsertJob (~500 lines, down from 1559).
 *
 * All source-specific logic is delegated to the TableStream.
 * All deployment-mode logic is delegated to MetadataPersistenceStrategy.
 * The job only handles: lifecycle, scheduling coordination,
 * transaction callbacks, persistence, and display.
 */
public class StreamingInsertJob extends AbstractJob<StreamingJobSchedulerTask, Map<Object, Object>>
        implements TxnStateChangeCallback, GsonPostProcessable {

    @SerializedName("st")
    private String streamType;          // "kafka", "s3", "mysql_cdc", etc.

    private TableStream<?, ?> tableStream;  // THE core source delegation target (transient)

    @SerializedName("props")
    private Map<String, String> properties;

    private StreamingJobProperties jobProperties;

    @SerializedName("cps")
    private String cursorPersistState;  // serialized cursor for persistence

    // Task management (generic, no source-specific maps)
    private Map<String, AbstractStreamingTask> activeTasks = new ConcurrentHashMap<>();

    // ... lifecycle, statistics, lock (same as before, but no Kafka-specific fields)

    /** Create initial tasks -- FULLY GENERIC, no source checks */
    protected void createInitialTasks() {
        TaskPartitioner<?, ?> partitioner = tableStream.getTaskPartitioner();
        List<? extends WorkUnit> workUnits = partitioner.partitionWork(
                tableStream.getCursor(), tableStream.getMetadata());
        for (WorkUnit wu : workUnits) {
            createAndRegisterTask(wu);
        }
    }

    /** Task completion -- FULLY GENERIC */
    public void onWorkUnitCompleted(WorkUnit workUnit, TaskResult result) {
        writeLock();
        try {
            tableStream.onTaskCompleted(workUnit, result);
            activeTasks.remove(workUnit.getWorkUnitId());
            succeedTaskCount.incrementAndGet();

            // Create next task in the pipeline
            WorkUnit next = tableStream.getTaskPartitioner()
                    .nextWorkUnit(workUnit, result, tableStream.getCursor(), tableStream.getMetadata());
            if (next != null) {
                createAndRegisterTask(next);
            }

            persistCursorIfNeeded();
        } finally {
            writeUnlock();
        }
    }

    /** Periodic check -- FULLY GENERIC */
    public void handleRunningState() {
        tableStream.refreshMetadata();

        // Restart idle pipelines (no-op for S3/JDBC, checks idle partitions for Kafka)
        List<? extends WorkUnit> restartable = tableStream.getTaskPartitioner()
                .findRestartableWork(activeTasks.keySet(),
                        tableStream.getCursor(), tableStream.getMetadata());
        for (WorkUnit wu : restartable) {
            createAndRegisterTask(wu);
        }
    }

    /** Transaction callback -- GENERIC, uses WorkUnit from task */
    @Override
    public void beforeCommitted(TransactionState txnState) {
        // Find the task by label, get its work unit, attach offset
        // No source-specific casting needed
    }

    /** Persistence -- GENERIC */
    private void persistCursorIfNeeded() {
        this.cursorPersistState = tableStream.serializeCursor();
        if (cursorPersistState != null) {
            logUpdateOperation();
        }
    }

    /** Recovery -- GENERIC */
    @Override
    public void gsonPostProcess() {
        this.tableStream = TableStreamFactory.create(streamType);
        if (cursorPersistState != null) {
            tableStream.restoreCursor(cursorPersistState);
        }
    }

    /** Statistics update -- UNIFIED (no cloud/non-cloud branching) */
    private void updateJobStatisticAndOffset(StreamingTaskTxnCommitAttachment attachment) {
        // Delegate to persistence strategy - handles accumulate vs replace
        this.jobStatistic = MetadataPersistenceFactory.getInstance()
                .updateStatistics(this.jobStatistic, attachment);

        tableStream.updateCursor(
            tableStream.getCursor().fromOffset(attachment.getOffset()));
    }

    /** Offset modification -- UNIFIED */
    private void modifyPropertiesInternal(Map<String, String> inputProperties)
            throws AnalysisException, JobException {
        // ...
        if (StringUtils.isNotEmpty(inputStreamProps.getOffsetProperty()) && this.tvfType != null) {
            Offset offset = validateOffset(inputStreamProps.getOffsetProperty());
            tableStream.updateCursor(tableStream.getCursor().fromOffset(offset));
            // Delegate to persistence strategy
            MetadataPersistenceFactory.getInstance().resetOffset(getDbId(), getJobId(), offset);
        }
        // ...
    }

    /** Cleanup -- UNIFIED */
    public void cleanup() throws JobException {
        // Delegate to persistence strategy (handles cloud cleanup if needed)
        if (tvfType != null) {
            MetadataPersistenceFactory.getInstance().cleanup(getDbId(), getJobId());
        }
        // Source-specific cleanup
        tableStream.cleanup();
    }
}
```

### 5.2 StreamingJobSchedulerTask (Sketch)

```java
/**
 * Fully generic scheduler task. No source-type or deployment-mode checks.
 */
public class StreamingJobSchedulerTask extends AbstractTask {

    @Override
    public void run() throws JobException {
        switch (streamingInsertJob.getJobStatus()) {
            case PENDING:
                handlePendingState();
                break;
            case RUNNING:
                handleRunningState();
                break;
            case PAUSED:
                autoResumeHandler();
                break;
        }
    }

    private void handlePendingState() throws JobException {
        // Delegate to persistence strategy - no mode check needed
        try {
            MetadataPersistenceFactory.getInstance()
                    .recoverJobState(streamingInsertJob);
        } catch (JobException e) {
            streamingInsertJob.setFailureReason(
                    new FailureReason(InternalErrorCode.INTERNAL_ERR, e.getMessage()));
            streamingInsertJob.updateJobStatus(JobStatus.PAUSED);
            return;
        }

        // Restore cursor from persistence (generic -- delegates to TableStream)
        streamingInsertJob.restoreCursorIfNeeded();
        // Refresh metadata (generic -- delegates to TableStream)
        streamingInsertJob.getTableStream().refreshMetadata();
        // Create initial tasks (generic -- uses TaskPartitioner)
        streamingInsertJob.createInitialTasks();
        streamingInsertJob.updateJobStatus(JobStatus.RUNNING);
        streamingInsertJob.setAutoResumeCount(0);
    }

    private void handleRunningState() throws JobException {
        // Process timeouts (generic)
        streamingInsertJob.processTimeoutTasks();
        // Refresh metadata and restart idle pipelines (generic)
        streamingInsertJob.handleRunningState();
    }

    /** Replay on updated -- UNIFIED */
    public void replayOnUpdated(StreamingInsertJob replayJob) {
        // ...
        try {
            modifyPropertiesInternal(replayJob.getProperties());
            // Delegate to persistence strategy
            if (MetadataPersistenceFactory.getInstance().needRecoveryOnReplay()) {
                MetadataPersistenceFactory.getInstance().recoverJobState(this);
            }
        } catch (Exception e) {
            // ...
        }
        // ...
    }
}
```

---

## 6. Data Flow by Source Type

### 6.1 Kafka Data Flow

```
1. Job created -> TableStreamFactory.create("kafka") -> KafkaTableStream
2. PENDING: refreshMetadata() fetches partition list & latest offsets
3. partitionWork() returns one KafkaPartitionWork per partition
4. Each partition runs independently:
   - task completes -> onTaskCompleted() updates partition cursor
   - nextWorkUnit() returns next batch for same partition
   - If no more data: returns null (pipeline stops, partition becomes idle)
5. RUNNING state periodic check:
   - refreshMetadata() updates latest offsets
   - findRestartableWork() finds idle partitions with new data
   - Creates new tasks for restarted partitions
```

### 6.2 S3 Data Flow

```
1. Job created -> S3TableStream
2. PENDING: refreshMetadata() lists available files
3. partitionWork() returns single S3BatchWork with next batch of files
4. Sequential execution:
   - task completes -> onTaskCompleted() advances file cursor
   - nextWorkUnit() returns next file batch
5. findRestartableWork() returns empty (no parallel pipelines)
```

### 6.3 JDBC/CDC Data Flow

```
1. Job created -> JdbcTableStream
2. PENDING: Splits tables into chunks, initializes snapshot splits
3. partitionWork() returns JdbcSnapshotWork for first split
4. Phase transitions:
   - Snapshot splits processed sequentially
   - After all splits: transitions to JdbcBinlogWork
   - Binlog mode: continuous incremental consumption
5. findRestartableWork() returns empty (single pipeline)
```

### 6.4 Cloud vs Non-Cloud Recovery Flow

```
Non-Cloud Mode:
1. FE restarts -> edit log replays StreamingInsertJob
2. gsonPostProcess() restores TableStream with persisted cursor
3. handlePendingState() -> LocalMetadataPersistence.recoverJobState() (no-op)
4. Job resumes from cursor in edit log

Cloud Mode:
1. FE restarts -> edit log replays StreamingInsertJob (partial state)
2. gsonPostProcess() restores TableStream
3. handlePendingState() -> CloudMetadataPersistence.recoverJobState()
   - RPC to cloud meta service
   - Fetches authoritative statistics and offset
   - Updates job state with cloud values
4. Job resumes from cloud-provided cursor
```

---

## 7. Extension Guide: Adding a New Source

To add a new data source (e.g., Pulsar), a developer needs to implement **6 classes**:

### Step 1: Create the package

```
org.apache.doris.job.stream.pulsar/
```

### Step 2: Implement 6 classes

**1. PulsarCursor implements StreamCursor**
```java
public class PulsarCursor implements StreamCursor {
    private Map<String, MessageId> topicPartitionPositions;

    @Override
    public String toJson() { /* serialize */ }

    @Override
    public boolean isInitial() { return topicPartitionPositions.isEmpty(); }

    @Override
    public boolean isValid() { return topicPartitionPositions != null; }

    @Override
    public String toDisplayString() { /* human-readable */ }
}
```

**2. PulsarWork implements WorkUnit**
```java
public class PulsarWork implements WorkUnit {
    private String topicPartition;
    private MessageId startId;
    private MessageId endId;

    @Override
    public String getWorkUnitId() { return "pulsar-" + topicPartition; }

    @Override
    public String toJson() { /* serialize */ }

    @Override
    public String toDisplayString() { /* e.g., "topic-0:[100,200)" */ }
}
```

**3. PulsarStreamMetadata implements StreamMetadata**
```java
public class PulsarStreamMetadata implements StreamMetadata {
    private Map<String, MessageId> latestPositions;
    private long refreshTimestamp;

    @Override
    public boolean isValid() { return System.currentTimeMillis() - refreshTimestamp < 60000; }
}
```

**4. PulsarTaskPartitioner implements TaskPartitioner**
```java
public class PulsarTaskPartitioner implements TaskPartitioner<PulsarCursor, PulsarWork> {

    @Override
    public List<PulsarWork> partitionWork(PulsarCursor cursor, StreamMetadata metadata) {
        // One work unit per topic-partition
    }

    @Override
    public PulsarWork nextWorkUnit(PulsarWork completed, TaskResult result,
                                    PulsarCursor cursor, StreamMetadata metadata) {
        // Next batch for the same partition
    }

    @Override
    public List<PulsarWork> findRestartableWork(Set<String> active,
                                                 PulsarCursor cursor, StreamMetadata metadata) {
        // Check idle partitions for new data
    }

    @Override
    public ParallelismModel getParallelismModel() {
        return ParallelismModel.PARALLEL_PIPELINES;
    }
}
```

**5. PulsarSqlRewriter implements SqlRewriter**
```java
public class PulsarSqlRewriter implements SqlRewriter<PulsarWork> {

    @Override
    public InsertIntoTableCommand rewrite(InsertIntoTableCommand base, PulsarWork work) {
        // Rewrite pulsar() TVF to bounded query with position filters
    }
}
```

**6. PulsarTableStream implements TableStream**
```java
public class PulsarTableStream implements TableStream<PulsarCursor, PulsarWork> {
    private PulsarCursor cursor;
    private PulsarStreamMetadata metadata;
    private PulsarTaskPartitioner partitioner;
    private PulsarSqlRewriter rewriter;

    @Override
    public void initialize(StreamConfig config) {
        // Connect to Pulsar, discover topics
    }

    @Override
    public void refreshMetadata() {
        // Fetch latest message IDs from Pulsar
    }

    @Override
    public AbstractStreamingTask createTask(PulsarWork work, StreamTaskContext ctx) {
        return new StreamingInsertTask(/* with rewritten SQL */);
    }

    // ... other interface methods
}
```

### Step 3: Register in TableStreamFactory

```java
// In TableStreamFactory.java
static {
    register("pulsar", PulsarTableStream.class);
}
```

### What a developer does NOT need to touch

| Component | Lines of Code | Needs Change? |
|-----------|---------------|---------------|
| StreamingInsertJob | ~500 (refactored) | NO |
| StreamingJobSchedulerTask | ~100 | NO |
| AbstractStreamingTask | ~170 | NO |
| StreamingInsertTask | ~200 | NO |
| Transaction callbacks | ~100 | NO |
| Persistence/replay | ~100 | NO |
| MetadataPersistenceStrategy | ~200 | NO |

---

## 8. Migration Path

### Phase 1: Introduce interfaces (non-breaking)
- Create `org.apache.doris.job.stream` package with all interfaces
- Create `org.apache.doris.job.stream.persistence` package with persistence strategy
- No behavioral changes to existing code

### Phase 2: Implement KafkaTableStream
- Create `org.apache.doris.job.stream.kafka` package
- Wrap existing `KafkaSourceOffsetProvider` logic into new classes
- Add feature flag: `use_table_stream_architecture = false` (default)
- When flag is ON, use new path; when OFF, use legacy

### Phase 3: Implement S3TableStream and JdbcTableStream
- Create respective packages
- Wrap existing provider logic into new classes
- Test with feature flag

### Phase 4: Implement MetadataPersistenceStrategy
- Create `LocalMetadataPersistence` and `CloudMetadataPersistence`
- Create `MetadataPersistenceFactory`
- Test both modes independently

### Phase 5: Refactor StreamingInsertJob
- Remove all `isKafkaStreamingJob()` checks
- Remove all `isCloudMode()` checks
- Remove `runningKafkaPartitionTasks`, `activePartitions` fields
- Replace `SourceOffsetProvider` with `TableStream`
- Replace cloud-specific code with `MetadataPersistenceStrategy`
- Remove source-specific callbacks and task creation methods
- Merge statistics update methods

### Phase 6: Refactor StreamingJobSchedulerTask
- Remove `isCloudMode()` check with strategy delegation
- Remove any source-specific logic

### Phase 7: Refactor StreamingInsertTask
- Remove `kafkaPartitionOffset` field
- Add generic `WorkUnit workUnit` field
- Remove source-specific `onSuccess()` branching

### Phase 8: Deprecate and remove old code
- Mark `SourceOffsetProvider` and implementations as `@Deprecated`
- Remove feature flag (make new architecture default)
- Delete deprecated classes after one release cycle

---

## 9. Persistence Compatibility

New `StreamCursor` implementations must handle legacy format deserialization:

```java
// In KafkaCursor
public static KafkaCursor fromLegacyJson(String json) {
    // Try new format first
    try {
        return GsonUtils.GSON.fromJson(json, KafkaCursor.class);
    } catch (Exception e) {
        // Fall back to legacy KafkaOffset format
        KafkaOffset legacy = GsonUtils.GSON.fromJson(json, KafkaOffset.class);
        return new KafkaCursor(legacy.getPartitionOffsets(), legacy.getTopic(), ...);
    }
}
```

The `StreamingTaskTxnCommitAttachment` format remains unchanged since it is stored in transaction state.

---

## 10. Key Design Decisions

| # | Decision | Rationale | Tradeoff |
|---|----------|-----------|----------|
| 1 | TableStream is stateful | Holds cursor and metadata state; simpler API (job calls `getCursor()` instead of passing objects around) | Slightly harder to test in isolation; mitigated by separating stateless TaskPartitioner |
| 2 | Generic type parameters `<C, W>` | Compile-time type safety between cursor, work unit, partitioner, rewriter | StreamingInsertJob uses `<?, ?>` wildcards; casts happen inside TableStream implementations |
| 3 | TaskPartitioner as separate interface | Separates "what data exists" from "how to divide it"; testable independently | One more interface; but captures the most fundamental source difference |
| 4 | WorkUnit.getWorkUnitId() as pipeline key | Replaces Kafka-specific `Integer partitionId`; generic across all sources | Slightly more verbose than integer keys |
| 5 | SqlRewriter is optional (nullable) | JDBC CDC uses gRPC, not SQL; forcing a rewriter would be artificial | Callers must null-check; only affects StreamingInsertTask.before() |
| 6 | Feature flag for migration | Safe rollout for production system; test new architecture incrementally | Temporarily more code (both paths); removed after validation |
| 7 | Keep AbstractStreamingTask | Solid lifecycle with retry logic; only remove source-specific fields | Keeps existing task structure; minimal disruption |
| 8 | Unified Interface for all sources | JdbcTableStream implements TableStream with null SqlRewriter; createTask() creates gRPC-based StreamingMultiTblTask | One interface for all; no separate hierarchy for non-SQL paths |
| 9 | MetadataPersistenceStrategy for deployment modes | Eliminates all `isCloudMode()` checks from business logic | One more abstraction layer; but enables clean testing and future modes |
| 10 | Strategy determined at startup | Single `getInstance()` call returns cached strategy | Cannot switch modes at runtime; but this is by design (deployment mode is fixed) |

---

## 11. Files to Modify

### Source Abstraction Refactoring

| File | Current Lines | Target Lines | Changes |
|------|---------------|--------------|---------|
| `StreamingInsertJob.java` | 1559 | ~500-600 | Remove all source-specific and cloud-specific logic |
| `StreamingJobSchedulerTask.java` | 133 | ~100 | Remove `isKafkaStreamingJob()` and `isCloudMode()` checks |
| `StreamingInsertTask.java` | ~270 | ~200 | Remove `kafkaPartitionOffset` field, use generic `WorkUnit` |
| `SourceOffsetProvider.java` | 115 | @Deprecated | Replaced by TableStream + StreamCursor + TaskPartitioner + SqlRewriter |
| `KafkaSourceOffsetProvider.java` | 768 | @Deprecated | Logic moves to KafkaTableStream components |
| `S3SourceOffsetProvider.java` | ~215 | @Deprecated | Logic moves to S3TableStream components |
| `JdbcSourceOffsetProvider.java` | ~580 | @Deprecated | Logic moves to JdbcTableStream components |

### New Files to Create

| Package | Files |
|---------|-------|
| `org.apache.doris.job.stream` | TableStream.java, StreamCursor.java, WorkUnit.java, TaskPartitioner.java, SqlRewriter.java, StreamMetadata.java, ParallelismModel.java, StreamConfig.java, StreamTaskContext.java, TaskResult.java, StreamDisplayInfo.java, StreamException.java, TableStreamFactory.java |
| `org.apache.doris.job.stream.persistence` | MetadataPersistenceStrategy.java, LocalMetadataPersistence.java, CloudMetadataPersistence.java, MetadataPersistenceFactory.java |
| `org.apache.doris.job.stream.kafka` | KafkaTableStream.java, KafkaCursor.java, KafkaPartitionWork.java, KafkaTaskPartitioner.java, KafkaSqlRewriter.java, KafkaStreamMetadata.java |
| `org.apache.doris.job.stream.s3` | S3TableStream.java, S3Cursor.java, S3BatchWork.java, S3TaskPartitioner.java, S3SqlRewriter.java, S3StreamMetadata.java |
| `org.apache.doris.job.stream.jdbc` | JdbcTableStream.java, JdbcCursor.java, JdbcSnapshotWork.java, JdbcBinlogWork.java, JdbcTaskPartitioner.java, JdbcSqlRewriter.java, JdbcStreamMetadata.java |

### Files to Eventually Delete

After migration is complete and validated:
- `SourceOffsetProvider.java`
- `KafkaSourceOffsetProvider.java`
- `S3SourceOffsetProvider.java`
- `JdbcSourceOffsetProvider.java`
- Methods: `replayOnCloudMode()`, `resetCloudProgress()`, `updateCloudJobStatisticAndOffset()`

---

## 12. Verification Plan

### Unit Tests
1. Test each new interface implementation in isolation
2. Test LocalMetadataPersistence with mock job
3. Test CloudMetadataPersistence with mock RPC
4. Test TableStreamFactory registration and creation

### Integration Tests
1. **Kafka**: verify per-partition parallelism, idle restart, offset persistence
2. **S3**: verify sequential batch processing, file cursor
3. **JDBC**: verify snapshot->binlog phase transition, split recovery
4. **Cloud Mode**: verify recovery from cloud meta service
5. **Non-Cloud Mode**: verify recovery from edit log

### Regression Tests
1. Run all existing streaming job tests with feature flag ON
2. Verify SHOW STREAMING JOBS output unchanged
3. Verify ALTER JOB (offset modification) still works
4. Verify no `isCloudMode()` or `isKafkaStreamingJob()` remains in refactored code

### Manual Testing
1. Create streaming job for each source type
2. Verify SHOW STREAMING JOBS output
3. Kill FE, restart, verify offset recovery
4. Test both cloud and non-cloud deployments

### Performance Testing
1. Compare task creation/completion latency before/after
2. Verify no regression in throughput

---

## 13. Implementation TODO List

### Phase 1: Core Interfaces
- [ ] Create `org.apache.doris.job.stream` package
- [ ] Define `TableStream<C, W>` interface
- [ ] Define `StreamCursor` interface
- [ ] Define `WorkUnit` interface
- [ ] Define `TaskPartitioner<C, W>` interface
- [ ] Define `SqlRewriter<W>` interface
- [ ] Define `StreamMetadata` interface
- [ ] Create `ParallelismModel` enum
- [ ] Create `StreamConfig`, `StreamTaskContext`, `TaskResult`, `StreamDisplayInfo` classes
- [ ] Create `StreamException` class
- [ ] Create `TableStreamFactory` with registration mechanism

### Phase 2: Persistence Abstraction
- [ ] Create `org.apache.doris.job.stream.persistence` package
- [ ] Define `MetadataPersistenceStrategy` interface
- [ ] Implement `LocalMetadataPersistence`
- [ ] Implement `CloudMetadataPersistence`
- [ ] Create `MetadataPersistenceFactory`
- [ ] Write unit tests for persistence strategies

### Phase 3: Kafka Implementation
- [ ] Create `org.apache.doris.job.stream.kafka` package
- [ ] Implement `KafkaCursor` with legacy JSON compatibility
- [ ] Implement `KafkaPartitionWork`
- [ ] Implement `KafkaStreamMetadata`
- [ ] Implement `KafkaTaskPartitioner`
- [ ] Implement `KafkaSqlRewriter`
- [ ] Implement `KafkaTableStream`
- [ ] Register in `TableStreamFactory`
- [ ] Add feature flag `use_table_stream_architecture`
- [ ] Write unit tests

### Phase 4: S3 and JDBC Implementations
- [ ] Implement S3 package (S3Cursor, S3BatchWork, S3TaskPartitioner, etc.)
- [ ] Implement JDBC package (JdbcCursor, JdbcSnapshotWork, JdbcBinlogWork, etc.)
- [ ] Write unit tests

### Phase 5: Refactor Core Classes
- [ ] Refactor `StreamingInsertJob` to use TableStream
- [ ] Refactor `StreamingInsertJob` to use MetadataPersistenceStrategy
- [ ] Remove all `isKafkaStreamingJob()` checks
- [ ] Remove all `isCloudMode()` checks
- [ ] Merge statistics update methods
- [ ] Refactor `StreamingJobSchedulerTask` to be fully generic
- [ ] Refactor `StreamingInsertTask` to use WorkUnit
- [ ] Update transaction callbacks

### Phase 6: Testing and Validation
- [ ] Run all existing tests with feature flag ON
- [ ] Manual testing for all source types
- [ ] Manual testing for both cloud and non-cloud modes
- [ ] Verify no conditional deployment/source logic remains
- [ ] Performance comparison testing

### Phase 7: Deprecation and Cleanup
- [ ] Mark old classes as `@Deprecated`
- [ ] Remove feature flag (next release)
- [ ] Remove deprecated classes (next release cycle)
- [ ] Update documentation

---

## Appendix A: Code Transformation Examples

### A.1 Statistics Update (Before)

```java
// Two separate methods with different semantics
private void updateJobStatisticAndOffset(attachment) {
    // Non-cloud: ACCUMULATE
    this.jobStatistic.setScannedRows(
        this.jobStatistic.getScannedRows() + attachment.getScannedRows());
}

private void updateCloudJobStatisticAndOffset(attachment) {
    // Cloud: REPLACE
    this.jobStatistic.setScannedRows(attachment.getScannedRows());
}
```

### A.2 Statistics Update (After)

```java
// One unified method
private void updateJobStatisticAndOffset(StreamingTaskTxnCommitAttachment attachment) {
    // Delegate to persistence strategy - handles accumulate vs replace
    this.jobStatistic = MetadataPersistenceFactory.getInstance()
            .updateStatistics(this.jobStatistic, attachment);
}
```

### A.3 Recovery Flow (Before)

```java
private void handlePendingState() throws JobException {
    if (Config.isCloudMode()) {  // <-- CONDITIONAL
        try {
            streamingInsertJob.replayOnCloudMode();
        } catch (JobException e) {
            // ...
        }
    }
    streamingInsertJob.replayOffsetProviderIfNeed();
    // ...
}
```

### A.4 Recovery Flow (After)

```java
private void handlePendingState() throws JobException {
    // Delegate to persistence strategy - no mode check needed
    try {
        MetadataPersistenceFactory.getInstance()
                .recoverJobState(streamingInsertJob);
    } catch (JobException e) {
        streamingInsertJob.setFailureReason(
                new FailureReason(InternalErrorCode.INTERNAL_ERR, e.getMessage()));
        streamingInsertJob.updateJobStatus(JobStatus.PAUSED);
        return;
    }
    streamingInsertJob.restoreCursorIfNeeded();
    // ...
}
```

### A.5 Task Creation (Before)

```java
protected void createInitialTasks() {
    if (isKafkaStreamingJob()) {  // <-- CONDITIONAL
        for (Integer partitionId : kafkaPartitions) {
            createKafkaPartitionTask(partitionId);
        }
    } else {
        createSingleTask();
    }
}
```

### A.6 Task Creation (After)

```java
protected void createInitialTasks() {
    // FULLY GENERIC - no source checks
    TaskPartitioner<?, ?> partitioner = tableStream.getTaskPartitioner();
    List<? extends WorkUnit> workUnits = partitioner.partitionWork(
            tableStream.getCursor(), tableStream.getMetadata());
    for (WorkUnit wu : workUnits) {
        createAndRegisterTask(wu);
    }
}
```

---

## Appendix B: Benefits Summary

| Aspect | Before | After |
|--------|--------|-------|
| **Source conditionals** | 16+ `isKafkaStreamingJob()` checks | 0 |
| **Cloud conditionals** | 4 `isCloudMode()` checks | 0 |
| **StreamingInsertJob size** | 1559 lines | ~500-600 lines |
| **Adding new source** | Modify 5+ existing files | Create 6 new classes, register |
| **Adding new deployment mode** | Scatter changes across files | Create 1 strategy class |
| **Testability** | Requires full job setup | Strategy/TableStream can be mocked |
| **Code paths** | Multiple conditional branches | Single generic path with delegation |
| **Type safety** | Runtime casts, null checks | Compile-time generics |
