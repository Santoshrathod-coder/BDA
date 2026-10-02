package problem5;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * NetConnect CDR Analysis - Problem 5: Plan Drop Rate
 * Reducer: Computes the proportion of dropped calls for each plan,
 * and identifies the plan with the highest dropped-call proportion.
 */
public class PlanDropRateReducer extends Reducer<Text, Text, Text, Text> {

    private final Text resultVal = new Text();

    private String highestPlan = "N/A";
    private long highestTotal = 0;
    private long highestDropped = 0;
    private double maxDropRate = -1.0;

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

        // Floating-point division
        double dropRate = totalCalls > 0 ? ((double) droppedCalls / (double) totalCalls) : 0.0;

        // Check if this plan has the highest drop rate proportion
        if (dropRate > maxDropRate) {
            maxDropRate = dropRate;
            highestPlan = key.toString();
            highestTotal = totalCalls;
            highestDropped = droppedCalls;
        }

        // Output row: totalCalls \t droppedCalls \t dropRate
        String row = String.format(Locale.US, "%d\t%d\t%.4f", totalCalls, droppedCalls, dropRate);
        resultVal.set(row);
        context.write(key, resultVal);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("----------------------------------------------------------------"), new Text("------------------------------"));
        context.write(
            new Text("Highest Drop Rate Plan: " + highestPlan),
            new Text(String.format(Locale.US, "Total Calls: %d\tDropped Calls: %d\tDrop Rate: %.4f",
                     highestTotal, highestDropped, maxDropRate))
        );
    }
}
