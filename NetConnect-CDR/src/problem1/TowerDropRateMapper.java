package problem1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * NetConnect CDR Analysis - Problem 1: Tower Drop Rate
 * Mapper: Extracts towerId as key and dropped flag ('Y' or 'N') as value.
 * Safely skips header and malformed records.
 */
public class TowerDropRateMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text towerKey = new Text();
    private final Text droppedVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();

        // Skip blank lines
        if (line.isEmpty()) {
            return;
        }

        // Skip CSV header line
        if (line.startsWith("callId") || line.contains("towerId")) {
            return;
        }

        // CSV schema: callId,towerId,callerPlan,durationSec,dropped,timestamp
        String[] fields = line.split(",");
        if (fields.length >= 5) {
            String towerId = fields[1].trim();
            String dropped = fields[4].trim().toUpperCase();

            // Validate towerId and dropped flag
            if (!towerId.isEmpty() && (dropped.equals("Y") || dropped.equals("N"))) {
                towerKey.set(towerId);
                droppedVal.set(dropped);
                context.write(towerKey, droppedVal);
            }
        }
    }
}
