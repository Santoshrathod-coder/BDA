-- ====================================================================
-- NetConnect CDR Analysis - Problem 6: Hive Implementation
-- Description: Tower-wise call-drop rate analysis using RCFILE and custom UDF.
-- ====================================================================

-- Step 1: Set execution configurations for MapReduce / Tez
SET hive.execution.engine=mr;
SET mapreduce.job.reduces=1;

-- Step 2: Create a Database for the project
CREATE DATABASE IF NOT EXISTS netconnect_db;
USE netconnect_db;

-- Step 3: Create Staging Table to load raw CSV data from HDFS
-- Skips the CSV header row automatically
DROP TABLE IF EXISTS cdr_staging;
CREATE EXTERNAL TABLE IF NOT EXISTS cdr_staging (
    callId STRING,
    towerId STRING,
    callerPlan STRING,
    durationSec INT,
    dropped STRING,
    `timestamp` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/netconnect/cdr/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Step 4: Create target table stored in RCFILE format
-- RCFile (Record Columnar File) provides row-group horizontal partitioning
-- and column-wise vertical storage for high compression and fast scan speed.
DROP TABLE IF EXISTS cdr_rcfile;
CREATE TABLE IF NOT EXISTS cdr_rcfile (
    callId STRING,
    towerId STRING,
    callerPlan STRING,
    durationSec INT,
    dropped STRING,
    `timestamp` STRING
)
STORED AS RCFILE;

-- Step 5: Populate the RCFILE table from staging table
INSERT OVERWRITE TABLE cdr_rcfile
SELECT callId, towerId, callerPlan, durationSec, dropped, `timestamp`
FROM cdr_staging;

-- Step 6: Verify RCFILE table structure and row count
DESCRIBE FORMATTED cdr_rcfile;
SELECT COUNT(*) AS total_records_in_rcfile FROM cdr_rcfile;
SELECT * FROM cdr_rcfile LIMIT 5;

-- Step 7: Register the custom Java UDF
-- Note: Update the path to your compiled JAR file if running from another directory
ADD JAR ./target/netconnect-cdr-1.0.jar;
CREATE TEMPORARY FUNCTION dropRate AS 'hive.DropRateUDF';

-- Step 8: Execute Tower Drop Rate Analysis using Custom UDF and CASE WHEN
-- Sorted by dropRate descending (highest drop rate towers first)
SELECT 
    towerId,
    COUNT(*) AS totalCalls,
    SUM(CASE WHEN UPPER(TRIM(dropped)) = 'Y' THEN 1 ELSE 0 END) AS droppedCalls,
    ROUND(dropRate(COUNT(*), SUM(CASE WHEN UPPER(TRIM(dropped)) = 'Y' THEN 1 ELSE 0 END)), 4) AS dropRate
FROM cdr_rcfile
GROUP BY towerId
ORDER BY dropRate DESC;

-- Step 9 (Optional): Native HQL query without UDF for verification
SELECT 
    towerId,
    COUNT(*) AS totalCalls,
    SUM(CASE WHEN UPPER(TRIM(dropped)) = 'Y' THEN 1 ELSE 0 END) AS droppedCalls,
    ROUND(CAST(SUM(CASE WHEN UPPER(TRIM(dropped)) = 'Y' THEN 1 ELSE 0 END) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE), 4) AS dropRate
FROM cdr_rcfile
GROUP BY towerId
ORDER BY dropRate DESC;
