package problem3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * NetConnect CDR Analysis - Problem 3: Tower Call Volume
 * Reducer & Combiner: Sums all call counts per towerId.
 */
public class TowerCallVolumeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable totalVal = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        totalVal.set(sum);
        context.write(key, totalVal);
    }
}
