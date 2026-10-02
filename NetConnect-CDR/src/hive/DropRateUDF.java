package hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * NetConnect CDR Analysis - Problem 6: Hive Custom UDF
 * Calculates tower call-drop rate = droppedCalls / totalCalls.
 * 
 * Safely handles:
 * - Null arguments
 * - Zero total calls (avoids division by zero)
 * - BigInt (LongWritable) and Int (IntWritable) data types
 */
@Description(
    name = "dropRate",
    value = "_FUNC_(totalCalls, droppedCalls) - Returns the call-drop rate as a double (0.0 to 1.0)",
    extended = "Example:\n  SELECT dropRate(1000, 50) FROM table;\n  Result: 0.05"
)
public class DropRateUDF extends UDF {

    private final DoubleWritable result = new DoubleWritable();

    /**
     * Overload for LongWritable parameters (e.g. from COUNT(*) and SUM(...))
     */
    public DoubleWritable evaluate(LongWritable totalCalls, LongWritable droppedCalls) {
        if (totalCalls == null || droppedCalls == null) {
            return null;
        }

        long total = totalCalls.get();
        long dropped = droppedCalls.get();

        if (total <= 0) {
            result.set(0.0);
            return result;
        }

        double rate = (double) dropped / (double) total;
        result.set(rate);
        return result;
    }

    /**
     * Overload for IntWritable parameters
     */
    public DoubleWritable evaluate(IntWritable totalCalls, IntWritable droppedCalls) {
        if (totalCalls == null || droppedCalls == null) {
            return null;
        }

        int total = totalCalls.get();
        int dropped = droppedCalls.get();

        if (total <= 0) {
            result.set(0.0);
            return result;
        }

        double rate = (double) dropped / (double) total;
        result.set(rate);
        return result;
    }

    /**
     * Overload for standard Java Long objects
     */
    public Double evaluate(Long totalCalls, Long droppedCalls) {
        if (totalCalls == null || droppedCalls == null) {
            return null;
        }

        if (totalCalls <= 0) {
            return 0.0;
        }

        return (double) droppedCalls / (double) totalCalls;
    }
}
