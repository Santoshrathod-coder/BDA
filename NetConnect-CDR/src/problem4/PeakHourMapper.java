package problem4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * NetConnect CDR Analysis - Problem 4: Peak Call Load by Time of Day
 * Mapper: Safely extracts the hour (00-23) from the timestamp field
 * and emits (hour, 1).
 */
public class PeakHourMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);
    private final Text hourKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();

        // Skip blank lines
        if (line.isEmpty()) {
            return;
        }

        // Skip CSV header
        if (line.startsWith("callId") || line.contains("timestamp")) {
            return;
        }

        // CSV schema: callId,towerId,callerPlan,durationSec,dropped,timestamp
        String[] fields = line.split(",");
        if (fields.length >= 6) {
            String timestamp = fields[5].trim();
            // Expected format: YYYY-MM-DD HH:mm:ss
            try {
                String[] dateAndTime = timestamp.split(" ");
                if (dateAndTime.length >= 2) {
                    String time = dateAndTime[1].trim();
                    String[] timeParts = time.split(":");
                    if (timeParts.length >= 1) {
                        String hourStr = timeParts[0].trim();
                        int hour = Integer.parseInt(hourStr);
                        if (hour >= 0 && hour <= 23) {
                            hourKey.set(String.format("%02d", hour));
                            context.write(hourKey, ONE);
                        }
                    }
                }
            } catch (Exception e) {
                // Skip invalid or unparseable timestamps safely
            }
        }
    }
}
