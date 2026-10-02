package problem3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * NetConnect CDR Analysis - Problem 3: Tower Call Volume
 * Mapper: Extracts towerId as key and emits constant 1 as value.
 */
public class TowerCallVolumeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);
    private final Text towerKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();

        // Skip blank lines
        if (line.isEmpty()) {
            return;
        }

        // Skip CSV header
        if (line.startsWith("callId") || line.contains("towerId")) {
            return;
        }

        // CSV schema: callId,towerId,callerPlan,durationSec,dropped,timestamp
        String[] fields = line.split(",");
        if (fields.length >= 2) {
            String towerId = fields[1].trim();
            if (!towerId.isEmpty()) {
                towerKey.set(towerId);
                context.write(towerKey, ONE);
            }
        }
    }
}
