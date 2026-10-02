package problem4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * NetConnect CDR Analysis - Problem 4: Peak Call Load by Time of Day
 * Reducer: Aggregates call counts per hour and identifies the peak call load hour.
 */
public class PeakHourReducer extends Reducer<Text, IntWritable, Text, Text> {

    private final Text countVal = new Text();
    private String peakHour = "N/A";
    private int maxCalls = -1;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Check if this hour is the new peak
        if (sum > maxCalls) {
            maxCalls = sum;
            peakHour = key.toString();
        }

        countVal.set(String.valueOf(sum));
        context.write(key, countVal);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Clearly report the peak hour and total calls as required
        context.write(new Text("----------------------------------------"), new Text("----------"));
        context.write(new Text("Peak Hour: " + peakHour), new Text("Total Calls: " + maxCalls));
    }
}
