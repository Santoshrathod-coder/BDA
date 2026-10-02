package problem1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * NetConnect CDR Analysis - Problem 1: Tower Drop Rate Driver
 * Submits the MapReduce job to YARN.
 *
 * Usage:
 *   hadoop jar netconnect-cdr-1.0.jar problem1.TowerDropRateDriver <inputPath> <outputPath>
 */
public class TowerDropRateDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: problem1.TowerDropRateDriver <inputPath> <outputPath>");
            System.err.println("Example: hadoop jar netconnect-cdr-1.0.jar problem1.TowerDropRateDriver /netconnect/cdr /netconnect/output/problem1");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NetConnect - Tower Drop Rate Analysis");

        job.setJarByClass(TowerDropRateDriver.class);
        job.setMapperClass(TowerDropRateMapper.class);
        job.setReducerClass(TowerDropRateReducer.class);

        // Map and Reduce output key-value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
