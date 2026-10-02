package problem2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * NetConnect CDR Analysis - Problem 2: Average Call Duration by Plan
 * Mapper: Extracts callerPlan as key and durationSec as value.
 */
public class PlanAverageDurationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final Text planKey = new Text();
    private final LongWritable durationVal = new LongWritable();

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
        if (fields.length >= 4) {
            String callerPlan = fields[2].trim();
            String durationStr = fields[3].trim();

            if (!callerPlan.isEmpty() && !durationStr.isEmpty()) {
                try {
                    long duration = Long.parseLong(durationStr);
                    if (duration >= 0) {
                        planKey.set(callerPlan);
                        durationVal.set(duration);
                        context.write(planKey, durationVal);
                    }
                } catch (NumberFormatException e) {
                    // Skip malformed numeric lines safely
                }
            }
        }
    }
}
