# YARN Schedulers Configuration Guide

This directory contains the custom scheduler definitions for the **NetConnect CDR Analysis Project**:

1. `capacity-scheduler.xml` – Queue definitions for Apache Hadoop Capacity Scheduler.
2. `fair-scheduler.xml` – Allocation definitions for Apache Hadoop Fair Scheduler.

---

## 1. How to Enable Capacity Scheduler in YARN

Capacity Scheduler is the default scheduler in Apache Hadoop 3.x.

### Step 1.1: Verify in `yarn-site.xml`
In `$HADOOP_CONF_DIR/yarn-site.xml` (usually `/usr/local/hadoop/etc/hadoop/yarn-site.xml` or `%HADOOP_HOME%\etc\hadoop\yarn-site.xml`):

```xml
<property>
  <name>yarn.resourcemanager.scheduler.class</name>
  <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
</property>
```

### Step 1.2: Apply `capacity-scheduler.xml`
Copy our custom `capacity-scheduler.xml` into the Hadoop configuration directory:

```bash
cp config/capacity-scheduler.xml $HADOOP_CONF_DIR/
```

### Step 1.3: Refresh Queues (Without Restarting YARN!)
```bash
yarn rmadmin -refreshQueues
```

Or restart YARN:
```bash
stop-yarn.sh && start-yarn.sh
```

### Step 1.4: Submit Job to Capacity Scheduler Queue
```bash
hadoop jar target/netconnect-cdr-1.0.jar problem1.TowerDropRateDriver \
  -Dmapreduce.job.queuename=telecom_analytics \
  /netconnect/cdr /netconnect/output/problem1_capacity
```

---

## 2. How to Enable Fair Scheduler in YARN

### Step 2.1: Configure `yarn-site.xml` for Fair Scheduler
In `$HADOOP_CONF_DIR/yarn-site.xml`:

```xml
<property>
  <name>yarn.resourcemanager.scheduler.class</name>
  <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value>
</property>
<property>
  <name>yarn.scheduler.fair.allocation.file</name>
  <value>/etc/hadoop/conf/fair-scheduler.xml</value>
  <!-- Replace with your absolute path to fair-scheduler.xml -->
</property>
<property>
  <name>yarn.scheduler.fair.preemption</name>
  <value>true</value>
</property>
```

### Step 2.2: Place `fair-scheduler.xml`
Copy `fair-scheduler.xml` to the path specified in `yarn.scheduler.fair.allocation.file`:
```bash
cp config/fair-scheduler.xml /etc/hadoop/conf/
```

### Step 2.3: Restart YARN ResourceManager
```bash
stop-yarn.sh
start-yarn.sh
```

### Step 2.4: Submit Job to Fair Scheduler Queue
```bash
hadoop jar target/netconnect-cdr-1.0.jar problem1.TowerDropRateDriver \
  -Dmapreduce.job.queuename=root.telecom_analytics \
  /netconnect/cdr /netconnect/output/problem1_fair
```

---

## 3. Monitoring in ResourceManager Web UI
Open your browser and navigate to:
```text
http://localhost:8088/cluster/scheduler
```
You can inspect:
- Active queues (`default` and `telecom_analytics`)
- Configured capacity vs. currently used capacity
- Running applications and containers allocated
- Fair share memory and vCores allocation
