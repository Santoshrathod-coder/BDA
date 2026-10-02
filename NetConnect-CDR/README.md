# NetConnect CDR Analysis Using Hadoop, MapReduce, YARN, Hive and Pig

A complete, production-grade Big Data engineering project for analyzing telecom Call Detail Records (CDRs) across distributed clusters.

---

## 1. Project Overview

Telecommunication carriers operate thousands of cellular towers and log millions of Call Detail Records (CDRs) every hour. This project implements a comprehensive distributed data processing pipeline on the Apache Hadoop ecosystem to solve six core telecom business analytics problems:
1. **Tower-Wise Call-Drop Rate** (Java MapReduce)
2. **Average Call Duration by Data Plan** (Java MapReduce)
3. **Total Call Volume by Tower** (Java MapReduce)
4. **Peak Call Load by Time of Day** (Java MapReduce)
5. **Highest Dropped-Call Proportion by Data Plan** (Java MapReduce)
6. **Multi-Engine Tower Drop Rate Analysis** (Apache Hive with RCFile & Custom Java UDF + Apache Pig Latin)
7. **YARN Scheduling Benchmark:** Capacity Scheduler vs. Fair Scheduler comparison

---

## 2. Project Directory Structure

```text
NetConnect-CDR/
│
├── data/
│   └── cdr.csv                      # Sample 100-record dataset with full diversity
│
├── src/
│   ├── problem1/
│   │   ├── TowerDropRateMapper.java
│   │   ├── TowerDropRateReducer.java
│   │   └── TowerDropRateDriver.java
│   │
│   ├── problem2/
│   │   ├── PlanAverageDurationMapper.java
│   │   ├── PlanAverageDurationReducer.java
│   │   └── PlanAverageDurationDriver.java
│   │
│   ├── problem3/
│   │   ├── TowerCallVolumeMapper.java
│   │   ├── TowerCallVolumeReducer.java
│   │   └── TowerCallVolumeDriver.java
│   │
│   ├── problem4/
│   │   ├── PeakHourMapper.java
│   │   ├── PeakHourReducer.java
│   │   └── PeakHourDriver.java
│   │
│   ├── problem5/
│   │   ├── PlanDropRateMapper.java
│   │   ├── PlanDropRateReducer.java
│   │   └── PlanDropRateDriver.java
│   │
│   └── hive/
│       └── DropRateUDF.java         # Custom Java Hive UDF
│
├── hive/
│   └── cdr_analysis.hql             # Hive script (RCFile table, UDF registration & queries)
│
├── pig/
│   └── cdr_drop_rate.pig            # Pig Latin script for drop rate aggregation
│
├── config/
│   ├── capacity-scheduler.xml       # Custom Capacity Scheduler queues
│   ├── fair-scheduler.xml           # Custom Fair Scheduler allocations
│   └── README.md                    # Detailed guide for toggling YARN schedulers
│
├── docs/
│   ├── project-report.md            # Complete 26-section college academic project report
│   └── viva-questions.md            # Comprehensive viva questions and answers
│
├── output/                          # Reference outputs for all 6 problems
│   ├── problem1/part-r-00000
│   ├── problem2/part-r-00000
│   ├── problem3/part-r-00000
│   ├── problem4/part-r-00000
│   ├── problem5/part-r-00000
│   └── problem6_pig/part-r-00000
│
├── README.md                        # Master execution and instructions guide
└── pom.xml                          # Maven build configuration
```

---

## 3. Technologies Used

- **Java Development Kit (JDK 8 or 11)**
- **Apache Hadoop (HDFS & MapReduce v2)**
- **Apache Hadoop YARN** (Capacity Scheduler & Fair Scheduler)
- **Apache Hive (HiveQL, RCFile storage format, Custom Java UDF)**
- **Apache Pig (Pig Latin procedural dataflow)**
- **Apache Maven** (Automated compilation and packaging)

---

## 4. Dataset Specification

File: `data/cdr.csv`  
Format: Comma-Separated Values (CSV)  

### Schema
```text
callId,towerId,callerPlan,durationSec,dropped,timestamp
```

### Fields
- `callId`: Unique identifier for each call transaction (`C001`, `C002`, ...)
- `towerId`: Cell tower servicing the call (`T01`, `T02`, `T03`, `T04`, `T05`)
- `callerPlan`: Customer subscription tier (`Prepaid`, `Postpaid`, `Premium`)
- `durationSec`: Duration in seconds (`120`, `300`, ...)
- `dropped`: Call drop status (`Y` for dropped call, `N` for successful call)
- `timestamp`: Call start time (`YYYY-MM-DD HH:mm:ss`)

---

## 5. Prerequisites

Before executing the commands, ensure the following are installed and configured in your environment:
1. **Java:** `java -version` (Java 8 or 11 recommended)
2. **Hadoop:** `hadoop version` (Hadoop 3.x or 2.x)
3. **Maven:** `mvn -version` (or use manual `javac` instructions below)
4. **Hive:** `hive --version` (for Problem 6 Hive)
5. **Pig:** `pig -version` (for Problem 6 Pig)

Environment variables should be set (e.g. in `~/.bashrc`):
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HIVE_HOME=/usr/local/hive
export PIG_HOME=/usr/local/pig
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$PIG_HOME/bin
```

---

## 6. Hadoop Daemons Setup

### 6.1 Start HDFS and YARN
```bash
start-dfs.sh
start-yarn.sh
```

### 6.2 Check Active Daemons
```bash
jps
```
Expected active processes:
```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

---

## 7. HDFS Setup & Data Ingestion

### 7.1 Create Project HDFS Directories
```bash
hdfs dfs -mkdir -p /netconnect/cdr
hdfs dfs -mkdir -p /netconnect/output
```

### 7.2 Upload the Dataset to HDFS
Navigate to the `NetConnect-CDR` directory and upload `cdr.csv`:
```bash
hdfs dfs -put data/cdr.csv /netconnect/cdr/
```

### 7.3 Verify Dataset in HDFS
```bash
hdfs dfs -ls /netconnect/cdr/
hdfs dfs -cat /netconnect/cdr/cdr.csv | head -n 10
```

---

## 8. Compilation & JAR Packaging

### Option A: Using Maven (Recommended)
From the `NetConnect-CDR/` directory:
```bash
mvn clean package
```
This compiles all MapReduce classes and the Hive UDF, producing:
`target/netconnect-cdr-1.0.jar`

### Option B: Manual Compilation with `hadoop classpath`
If Maven is not installed on your cluster:
```bash
# Create build directory
mkdir -p build

# Compile all Java sources
javac -classpath $(hadoop classpath):$(hive -e "set" 2>/dev/null | grep hive.jar.path | cut -d= -f2) \
  -d build \
  src/problem1/*.java \
  src/problem2/*.java \
  src/problem3/*.java \
  src/problem4/*.java \
  src/problem5/*.java \
  src/hive/*.java

# Package into JAR
jar -cvf target/netconnect-cdr-1.0.jar -C build/ .
```

---

## 9. Problem 1 Execution – Tower Call-Drop Rate

Business Question: *Which cell towers have the highest call-drop rate?*

```bash
# Remove output directory if re-running
hdfs dfs -rm -r /netconnect/output/problem1

# Run Job
hadoop jar target/netconnect-cdr-1.0.jar problem1.TowerDropRateDriver \
  /netconnect/cdr/cdr.csv \
  /netconnect/output/problem1

# View Output
hdfs dfs -cat /netconnect/output/problem1/part-r-00000
```

---

## 10. Problem 2 Execution – Average Call Duration by Plan

Business Question: *What is the average call duration across subscription plans?*

```bash
hdfs dfs -rm -r /netconnect/output/problem2

hadoop jar target/netconnect-cdr-1.0.jar problem2.PlanAverageDurationDriver \
  /netconnect/cdr/cdr.csv \
  /netconnect/output/problem2

hdfs dfs -cat /netconnect/output/problem2/part-r-00000
```

---

## 11. Problem 3 Execution – Total Call Volume by Tower

Business Question: *What is the total call volume handled by each cell tower?*

```bash
hdfs dfs -rm -r /netconnect/output/problem3

hadoop jar target/netconnect-cdr-1.0.jar problem3.TowerCallVolumeDriver \
  /netconnect/cdr/cdr.csv \
  /netconnect/output/problem3

hdfs dfs -cat /netconnect/output/problem3/part-r-00000
```

---

## 12. Problem 4 Execution – Peak Call Load by Time of Day

Business Question: *At what hour of the day does the network experience peak call load?*

```bash
hdfs dfs -rm -r /netconnect/output/problem4

hadoop jar target/netconnect-cdr-1.0.jar problem4.PeakHourDriver \
  /netconnect/cdr/cdr.csv \
  /netconnect/output/problem4

hdfs dfs -cat /netconnect/output/problem4/part-r-00000
```

---

## 13. Problem 5 Execution – Highest Dropped-Call Proportion by Plan

Business Question: *Which subscription plan has the highest proportion of dropped calls?*

```bash
hdfs dfs -rm -r /netconnect/output/problem5

hadoop jar target/netconnect-cdr-1.0.jar problem5.PlanDropRateDriver \
  /netconnect/cdr/cdr.csv \
  /netconnect/output/problem5

hdfs dfs -cat /netconnect/output/problem5/part-r-00000
```

---

## 14. Problem 6 Execution – Apache Hive with RCFILE & Custom UDF

Business Question: *Reimplement Tower Drop Rate analysis using Apache Hive, RCFILE storage format, and a custom Java UDF.*

```bash
# Execute the complete Hive script
hive -f hive/cdr_analysis.hql
```

### Manual Interactive Hive Execution:
```bash
hive
```
Inside the Hive prompt:
```sql
USE netconnect_db;

-- Inspect RCFILE table
DESCRIBE FORMATTED cdr_rcfile;

-- Register Custom UDF
ADD JAR ./target/netconnect-cdr-1.0.jar;
CREATE TEMPORARY FUNCTION dropRate AS 'hive.DropRateUDF';

-- Run Query
SELECT 
    towerId,
    COUNT(*) AS totalCalls,
    SUM(CASE WHEN UPPER(TRIM(dropped)) = 'Y' THEN 1 ELSE 0 END) AS droppedCalls,
    ROUND(dropRate(COUNT(*), SUM(CASE WHEN UPPER(TRIM(dropped)) = 'Y' THEN 1 ELSE 0 END)), 4) AS dropRate
FROM cdr_rcfile
GROUP BY towerId
ORDER BY dropRate DESC;
```

---

## 15. Problem 6 Execution – Apache Pig Latin

Business Question: *Reimplement Tower Drop Rate analysis using Apache Pig Latin dataflow.*

```bash
# Remove Pig HDFS output directory if re-running
hdfs dfs -rm -r /netconnect/output/problem6_pig

# Run Pig Script in MapReduce mode
pig -x mapreduce pig/cdr_drop_rate.pig

# View Pig Output in HDFS
hdfs dfs -cat /netconnect/output/problem6_pig/part-r-00000
```

---

## 16. YARN Scheduler Comparison

### 16.1 Testing under Capacity Scheduler
1. Copy `config/capacity-scheduler.xml` to `$HADOOP_CONF_DIR/capacity-scheduler.xml`.
2. Refresh queues:
   ```bash
   yarn rmadmin -refreshQueues
   ```
3. Submit MapReduce job to the `telecom_analytics` queue:
   ```bash
   hdfs dfs -rm -r /netconnect/output/p1_capacity
   hadoop jar target/netconnect-cdr-1.0.jar problem1.TowerDropRateDriver \
     -Dmapreduce.job.queuename=telecom_analytics \
     /netconnect/cdr/cdr.csv \
     /netconnect/output/p1_capacity
   ```
4. View metrics in the YARN ResourceManager UI: `http://localhost:8088/cluster/scheduler`

### 16.2 Testing under Fair Scheduler
1. In `$HADOOP_CONF_DIR/yarn-site.xml`, configure:
   ```xml
   <property>
     <name>yarn.resourcemanager.scheduler.class</name>
     <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value>
   </property>
   <property>
     <name>yarn.scheduler.fair.allocation.file</name>
     <value>/etc/hadoop/conf/fair-scheduler.xml</value>
   </property>
   ```
2. Copy `config/fair-scheduler.xml` to the configured path.
3. Restart YARN:
   ```bash
   stop-yarn.sh && start-yarn.sh
   ```
4. Submit the identical MapReduce job to the `root.telecom_analytics` fair queue:
   ```bash
   hdfs dfs -rm -r /netconnect/output/p1_fair
   hadoop jar target/netconnect-cdr-1.0.jar problem1.TowerDropRateDriver \
     -Dmapreduce.job.queuename=root.telecom_analytics \
     /netconnect/cdr/cdr.csv \
     /netconnect/output/p1_fair
   ```
5. Observe resource allocation and fair sharing in `http://localhost:8088/cluster/scheduler`.

---

## 17. Viewing Outputs

All output directories contain standard Hadoop output part files (`part-r-00000`).  
To view all results at once:
```bash
echo "=== Problem 1: Tower Drop Rate ==="
hdfs dfs -cat /netconnect/output/problem1/part-r-00000

echo "=== Problem 2: Plan Average Duration ==="
hdfs dfs -cat /netconnect/output/problem2/part-r-00000

echo "=== Problem 3: Tower Call Volume ==="
hdfs dfs -cat /netconnect/output/problem3/part-r-00000

echo "=== Problem 4: Peak Call Load Hour ==="
hdfs dfs -cat /netconnect/output/problem4/part-r-00000

echo "=== Problem 5: Plan Drop Rate ==="
hdfs dfs -cat /netconnect/output/problem5/part-r-00000

echo "=== Problem 6: Pig Output ==="
hdfs dfs -cat /netconnect/output/problem6_pig/part-r-00000
```

---

## 18. Troubleshooting Common Issues

1. **`Output directory /netconnect/output/... already exists`**
   - Solution: Hadoop prevents accidental data overwriting. Delete the directory before re-running:
     ```bash
     hdfs dfs -rm -r /netconnect/output/<problem_dir>
     ```
2. **`java.lang.ClassNotFoundException`**
   - Solution: Ensure the fully qualified package name is used when running `hadoop jar`:
     `problem1.TowerDropRateDriver`, not `TowerDropRateDriver`.
3. **`HDFS SafeModeException: Cannot create directory ... Name node is in safe mode`**
   - Solution: Wait a few seconds for DataNodes to check in, or leave safemode manually:
     ```bash
     hdfs dfsadmin -safemode leave
     ```
4. **`Hive UDF ClassNotFoundException`**
   - Solution: Verify that `ADD JAR ./target/netconnect-cdr-1.0.jar;` was executed inside the Hive session prior to `CREATE TEMPORARY FUNCTION`.
5. **`Connection Refused on Port 8020 / 9000`**
   - Solution: NameNode daemon is stopped. Verify with `jps` and restart with `start-dfs.sh`.

---

## 19. Expected Results (Reference)

### Problem 1 (Tower Drop Rate)
```text
T01	23	4	0.1739
T02	24	16	0.6667
T03	20	5	0.2500
T04	17	1	0.0588
T05	16	9	0.5625
```

### Problem 2 (Plan Average Duration)
```text
Postpaid	285.38
Prepaid	145.39
Premium	473.18
```

### Problem 3 (Tower Call Volume)
```text
T01	23
T02	24
T03	20
T04	17
T05	16
```

### Problem 4 (Peak Call Load by Hour)
```text
07	1
08	9
09	6
10	5
11	4
12	3
13	3
14	4
15	3
16	3
17	3
18	7
19	31
20	7
21	5
22	5
23	1
----------------------------------------	----------
Peak Hour: 19	Total Calls: 31
```

### Problem 5 (Plan Drop Rate)
```text
Postpaid	40	28	0.7000
Prepaid	38	7	0.1842
Premium	22	0	0.0000
----------------------------------------------------------------	------------------------------
Highest Drop Rate Plan: Postpaid	Total Calls: 40	Dropped Calls: 28	Drop Rate: 0.7000
```
