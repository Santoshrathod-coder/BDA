package problem2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * NetConnect CDR Analysis - Problem 2: Average Call Duration Driver
 *
 * Usage:
 *   hadoop jar netconnect-cdr-1.0.jar problem2.PlanAverageDurationDriver <inputPath> <outputPath>
 */
public class PlanAverageDurationDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: problem2.PlanAverageDurationDriver <inputPath> <outputPath>");
            System.err.println("Example: hadoop jar netconnect-cdr-1.0.jar problem2.PlanAverageDurationDriver /netconnect/cdr /netconnect/output/problem2");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NetConnect - Average Call Duration by Plan");

        job.setJarByClass(PlanAverageDurationDriver.class);
        job.setMapperClass(PlanAverageDurationMapper.class);
        job.setReducerClass(PlanAverageDurationReducer.class);

        // Map output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
