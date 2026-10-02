package problem2;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * NetConnect CDR Analysis - Problem 2: Average Call Duration by Plan
 * Reducer: Calculates the average call duration for each callerPlan
 * using floating-point arithmetic.
 */
public class PlanAverageDurationReducer extends Reducer<Text, LongWritable, Text, Text> {

    private final Text avgVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long totalDuration = 0;
        long callCount = 0;

        for (LongWritable val : values) {
            totalDuration += val.get();
            callCount++;
        }

        // Floating-point division: duration / calls
        double averageDuration = callCount > 0 ? ((double) totalDuration / (double) callCount) : 0.0;

        // Output format: average duration rounded to 2 decimal places
        avgVal.set(String.format(Locale.US, "%.2f", averageDuration));
        context.write(key, avgVal);
    }
}
