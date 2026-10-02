package problem4;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * NetConnect CDR Analysis - Problem 4: Peak Call Load Driver
 *
 * Usage:
 *   hadoop jar netconnect-cdr-1.0.jar problem4.PeakHourDriver <inputPath> <outputPath>
 */
public class PeakHourDriver {

    /**
     * Local Combiner to optimize network transfer before sending to final Reducer.
     */
    public static class HourlyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: problem4.PeakHourDriver <inputPath> <outputPath>");
            System.err.println("Example: hadoop jar netconnect-cdr-1.0.jar problem4.PeakHourDriver /netconnect/cdr /netconnect/output/problem4");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NetConnect - Peak Call Load by Time of Day");

        job.setJarByClass(PeakHourDriver.class);
        job.setMapperClass(PeakHourMapper.class);
        job.setCombinerClass(HourlyCombiner.class);
        job.setReducerClass(PeakHourReducer.class);

        // Ensure 1 reducer so all hours are compared to identify the global peak
        job.setNumReduceTasks(1);

        // Map output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
