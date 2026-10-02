-- ====================================================================
-- NetConnect CDR Analysis - Problem 6: Pig Implementation
-- Script: cdr_drop_rate.pig
-- Description: Calculate tower-wise call-drop rate using Pig Latin.
-- ====================================================================

-- Step 1: Load CDR data from HDFS using PigStorage
raw_cdr = LOAD '/netconnect/cdr/cdr.csv' USING PigStorage(',') AS (
    callId:chararray,
    towerId:chararray,
    callerPlan:chararray,
    durationSec:int,
    dropped:chararray,
    timestamp:chararray
);

-- Step 2: Filter out the CSV header row and blank lines
cdr = FILTER raw_cdr BY (callId != 'callId') AND (towerId IS NOT NULL) AND (dropped IS NOT NULL);

-- Step 3: Group records by cell tower ID
grouped_towers = GROUP cdr BY towerId;

-- Step 4: Nested FOREACH to compute total calls, dropped calls, and drop rate
tower_metrics = FOREACH grouped_towers {
    -- Filter dropped calls inside the nested bag
    dropped_subset = FILTER cdr BY UPPER(TRIM(dropped)) == 'Y';
    
    -- Count total calls and dropped calls
    total_calls = COUNT(cdr);
    dropped_calls = COUNT(dropped_subset);
    
    GENERATE 
        group AS towerId,
        total_calls AS totalCalls,
        dropped_calls AS droppedCalls,
        (total_calls > 0 ? ((double)dropped_calls / (double)total_calls) : 0.0) AS dropRate;
};

-- Step 5: Order results by drop rate in descending order (highest drop rate first)
sorted_drop_rates = ORDER tower_metrics BY dropRate DESC;

-- Step 6: Store output into HDFS output directory
STORE sorted_drop_rates INTO '/netconnect/output/problem6_pig' USING PigStorage('\t');

-- Step 7: Display results on terminal console
DUMP sorted_drop_rates;
