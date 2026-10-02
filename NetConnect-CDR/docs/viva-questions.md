# NetConnect CDR Analysis - Comprehensive Viva Questions & Answers

This document contains essential viva-voce questions and clear, concise answers covering all aspects of the **NetConnect CDR Analysis Project**. It is structured by technology and problem area to help CSE students excel in lab evaluations, project presentations, and technical interviews.

---

## 1. Apache Hadoop & Distributed Architecture

### Q1: What is Apache Hadoop?
**Answer:** Apache Hadoop is an open-source framework designed for distributed storage and distributed processing of very large datasets (Big Data) across clusters of commodity hardware using simple programming models. Its two core components are HDFS (Hadoop Distributed File System) for storage and MapReduce/YARN for processing and resource management.

### Q2: What is HDFS and how does it store files?
**Answer:** HDFS (Hadoop Distributed File System) is a distributed, scalable, and fault-tolerant file system. It breaks large files into smaller chunks called **blocks** (default size is 128 MB in Hadoop 2.x/3.x) and replicates each block across multiple physical DataNodes (default replication factor is 3) to ensure high data availability and fault tolerance.

### Q3: What is the role of NameNode?
**Answer:** The NameNode is the master daemon in HDFS. It manages the file system namespace, maintains the directory tree, and tracks the metadata of all files (such as file permissions, block IDs, and block-to-DataNode mappings). It stores this metadata in memory (RAM) and persists it using `fsimage` and `edits` log files.

### Q4: What is the role of DataNode?
**Answer:** The DataNode is the slave daemon in HDFS that runs on worker machines. It stores and retrieves actual data blocks on local physical disks as instructed by the NameNode, and sends periodic **Block Reports** and **Heartbeats** (typically every 3 seconds) to the NameNode to confirm it is alive and operational.

### Q5: What is the Secondary NameNode? Is it a backup NameNode?
**Answer:** No, the Secondary NameNode is **not** an active backup or hot-standby for the NameNode. Its primary job is to periodically merge the NameNode's active `edits` log file with the `fsimage` checkpoint file (a process called *checkpointing*) to prevent the edits log from growing uncontrollably large.

### Q6: What is YARN?
**Answer:** YARN stands for **Yet Another Resource Negotiator**. Introduced in Hadoop 2.0, YARN separates resource management from the processing model. It acts as the architectural operating system of Hadoop, allocating compute resources (CPU cores and RAM) across the cluster for diverse processing engines like MapReduce, Spark, Tez, and Flink.

---

## 2. MapReduce Programming Model

### Q7: What is MapReduce?
**Answer:** MapReduce is a distributed programming paradigm for processing vast amounts of data in parallel. It breaks a computation into two main phases:
1. **Map Phase:** Filters, transforms, and projects input splits into intermediate `(key, value)` pairs.
2. **Reduce Phase:** Aggregates, summarizes, or computes values grouped by intermediate keys.

### Q8: What happens during the Shuffle and Sort phase?
**Answer:** The Shuffle and Sort phase is the internal Hadoop mechanism occurring between the Map and Reduce phases:
- **Partitioning:** Determines which Reducer partition receives which key.
- **Sorting:** Intermediate keys emitted by Mappers are sorted in ascending order before reaching the Reducers.
- **Shuffling:** The sorted `(key, value)` pairs are physically transferred across the network from the Mapper nodes to the corresponding Reducer nodes.

### Q9: What is a Combiner in MapReduce?
**Answer:** A Combiner (often called a "mini-reducer") runs locally on the Mapper node directly after the Map task. It performs intermediate local aggregation on mapper output to drastically reduce the volume of data transferred over the network during the Shuffle phase. In Problem 3, `TowerCallVolumeReducer` is also utilized as a Combiner.

### Q10: What is the role of the Driver class?
**Answer:** The Driver class contains the `main()` method. It configures the Hadoop `Job` object, specifies the Mapper, Combiner, and Reducer classes, sets the input/output key and value data types, configures input and output HDFS paths, and submits the job to YARN.

---

## 3. Project Problems 1–5 (MapReduce Analysis)

### Q11: How is the Call-Drop Rate calculated in Problem 1?
**Answer:**
$$\text{Drop Rate} = \frac{\text{Dropped Calls (where dropped = 'Y')}}{\text{Total Calls}}$$
In `TowerDropRateMapper`, the tower ID is emitted as the key and the dropped status (`'Y'` or `'N'`) as the value. `TowerDropRateReducer` counts total values and dropped values, performing floating-point division `((double) droppedCalls / totalCalls)` to prevent integer division truncation.

### Q12: Why is floating-point division critical in Problem 2 and Problem 5?
**Answer:** In Java, dividing two integers or longs (e.g., `5 / 20`) results in integer truncation (`0`). By casting to `double` (e.g., `(double) dropped / total`), we preserve decimal precision (`0.2500`), which is essential for accurate business calculations.

### Q13: How does Problem 3 calculate Tower Call Volume?
**Answer:** For every valid record, `TowerCallVolumeMapper` emits `(towerId, 1)`. The Reducer (and Combiner) sums all the ones for each `towerId`. The output represents the total number of calls handled by each tower.

### Q14: How does Problem 4 identify the Peak Call Load Hour?
**Answer:** The Mapper extracts the timestamp string (e.g., `2026-09-01 19:25:40`), splits the date and time, and isolates the two-digit hour `19`. It emits `(hour, 1)`. In `PeakHourReducer`, we configure `setNumReduceTasks(1)` so a single reducer receives all 24 hours. The reducer aggregates each hour's count and uses the `cleanup()` lifecycle method to print the overall peak hour.

### Q15: Why analyze dropped-call *proportion* rather than raw *count* in Problem 5?
**Answer:** A plan with 10,000 users might have 200 drops (2% drop rate), whereas a plan with 100 users might have 40 drops (40% drop rate). Raw counts are biased by user population size. Proportion (rate) accurately reflects the quality of service experienced by customers on each plan.

---

## 4. Hive, RCFile & Custom UDF (Problem 6)

### Q16: What is Apache Hive and HiveQL (HQL)?
**Answer:** Apache Hive is a distributed data warehouse software built on top of Apache Hadoop. It allows developers and data analysts familiar with SQL to query and summarize petabytes of data using a SQL-like declarative language called **HiveQL (HQL)**, which Hive automatically translates into MapReduce, Tez, or Spark execution DAGs.

### Q17: What is an RCFile and why is it used?
**Answer:** **RCFile** stands for *Record Columnar File*. It is a hybrid storage structure designed for MapReduce-based systems:
- It divides a table horizontally into multiple row groups.
- Within each row group, data is stored vertically in columnar format.
**Advantages:**
1. Fast column projection: Only columns needed in the `SELECT` query are read from disk.
2. High compression ratio: Identical data types in columns compress much more efficiently than row-based CSV/Text.
3. Skip-scanning: Blocks containing irrelevant data can be bypassed.

### Q18: What is a Hive UDF and how was it developed in this project?
**Answer:** A **User Defined Function (UDF)** extends Hive's built-in query processing capabilities with custom Java logic. In `DropRateUDF.java`, we extend `org.apache.hadoop.hive.ql.exec.UDF` and implement the `evaluate(LongWritable totalCalls, LongWritable droppedCalls)` method to calculate `droppedCalls / totalCalls`, safely handling null values and preventing division by zero.

### Q19: How do you register and use a custom Java UDF in Hive?
**Answer:**
```sql
ADD JAR /path/to/netconnect-cdr-1.0.jar;
CREATE TEMPORARY FUNCTION dropRate AS 'hive.DropRateUDF';
SELECT towerId, dropRate(COUNT(*), SUM(CASE WHEN dropped='Y' THEN 1 ELSE 0 END)) FROM cdr_rcfile GROUP BY towerId;
```

---

## 5. Apache Pig (Problem 6)

### Q20: What is Apache Pig and Pig Latin?
**Answer:** Apache Pig is a high-level dataflow platform for processing large datasets on Hadoop. It uses a procedural dataflow language called **Pig Latin**, where users describe a sequence of transformations (load, filter, group, foreach, store) rather than writing Java MapReduce boilerplate code.

### Q21: What is the difference between `GROUP` and `FOREACH` in Pig?
**Answer:**
- `GROUP`: Collects tuples that have identical key values into a composite data structure containing the key (`group`) and an inner bag of records.
- `FOREACH`: Iterates over the records (or grouped bags) to transform, project, or aggregate data using functions like `COUNT`, `SUM`, and conditional expressions.

### Q22: How does the Pig script filter out the CSV header?
**Answer:**
```pig
cdr = FILTER raw_cdr BY (callId != 'callId') AND (towerId IS NOT NULL);
```
Since the CSV header line starts with the literal string `'callId'`, checking `callId != 'callId'` neatly removes the header without dropping valid data rows.

---

## 6. YARN Schedulers & Resource Management

### Q23: What are the three main components of YARN?
**Answer:**
1. **ResourceManager (RM):** The global master daemon that arbitrates and tracks compute resources (RAM, CPU cores) across the entire cluster.
2. **NodeManager (NM):** The per-node worker agent responsible for launching, monitoring, and reporting the resource consumption of local containers.
3. **ApplicationMaster (AM):** A per-application master that negotiates appropriate resource containers from the ResourceManager and collaborates with NodeManagers to execute and monitor tasks.

### Q24: What is the Capacity Scheduler?
**Answer:** The Capacity Scheduler is designed for multi-tenant enterprise clusters where multiple organizations or business units share a cluster. It provides **guaranteed capacity** by allocating dedicated percentages of cluster resources to hierarchical queues (e.g., 70% to `telecom_analytics`, 30% to `default`), while allowing idle capacity to be temporarily borrowed by other queues (elasticity).

### Q25: What is the Fair Scheduler?
**Answer:** The Fair Scheduler dynamically assigns resources so that all running applications get, on average, an equal share of cluster capacity over time. If only one job runs, it gets the entire cluster. When a second job arrives, tasks from the first job finish and free containers are allocated to the second job until both achieve their fair share (configured via queue weights or minResources).

### Q26: What is the difference between Capacity Scheduler and Fair Scheduler?
**Answer:**
| Feature | Capacity Scheduler | Fair Scheduler |
| :--- | :--- | :--- |
| **Primary Goal** | Guaranteed capacity for multi-tenant teams | Equal/weighted resource sharing among all active jobs |
| **Resource Allocation** | Strict percentages configured per queue | Dynamic reassignment based on queue weights and job demand |
| **Preemption** | Optional, historically limited | Built-in preemption support to rebalance capacity |
| **Default in Hadoop** | Default scheduler in Hadoop 2.x and 3.x | Must be explicitly enabled in `yarn-site.xml` |

### Q27: How do you submit a MapReduce job to a specific YARN queue?
**Answer:** By supplying the generic configuration parameter `-Dmapreduce.job.queuename=<queue_name>`:
```bash
hadoop jar netconnect-cdr-1.0.jar problem1.TowerDropRateDriver \
  -Dmapreduce.job.queuename=telecom_analytics \
  /netconnect/cdr /netconnect/output/problem1
```

### Q28: How can you inspect YARN jobs and queues visually?
**Answer:** By opening the YARN ResourceManager Web UI at `http://localhost:8088/cluster/apps` or `http://localhost:8088/cluster/scheduler`. It displays application states, queue hierarchies, allocated memory, vCores, submission time, start time, and completion time.
