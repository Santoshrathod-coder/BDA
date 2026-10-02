package problem5;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * NetConnect CDR Analysis - Problem 5: Plan Drop Rate
 * Mapper: Extracts callerPlan as key and dropped flag ('Y' or 'N') as value.
 */
public class PlanDropRateMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text planKey = new Text();
    private final Text droppedVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();

        // Skip blank lines
        if (line.isEmpty()) {
            return;
        }

        // Skip CSV header
        if (line.startsWith("callId") || line.contains("callerPlan")) {
            return;
        }

        // CSV schema: callId,towerId,callerPlan,durationSec,dropped,timestamp
        String[] fields = line.split(",");
        if (fields.length >= 5) {
            String callerPlan = fields[2].trim();
            String dropped = fields[4].trim().toUpperCase();

            if (!callerPlan.isEmpty() && (dropped.equals("Y") || dropped.equals("N"))) {
                planKey.set(callerPlan);
                droppedVal.set(dropped);
                context.write(planKey, droppedVal);
            }
        }
    }
}
