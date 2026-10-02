package problem1;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * NetConnect CDR Analysis - Problem 1: Tower Drop Rate
 * Reducer: Aggregates total calls and dropped calls per tower,
 * and calculates call-drop rate using floating-point division.
 */
public class TowerDropRateReducer extends Reducer<Text, Text, Text, Text> {

    private final Text resultVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long totalCalls = 0;
        long droppedCalls = 0;

        for (Text val : values) {
            totalCalls++;
            if ("Y".equalsIgnoreCase(val.toString().trim())) {
                droppedCalls++;
            }
        }

        // Floating-point division to avoid integer truncation
        double dropRate = totalCalls > 0 ? ((double) droppedCalls / (double) totalCalls) : 0.0;

        // Output format: totalCalls \t droppedCalls \t dropRate
        String formattedOutput = String.format(Locale.US, "%d\t%d\t%.4f", totalCalls, droppedCalls, dropRate);
        resultVal.set(formattedOutput);

        context.write(key, resultVal);
    }
}
