package problem5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * NetConnect CDR Analysis - Problem 5: Plan Drop Rate Driver
 *
 * Usage:
 *   hadoop jar netconnect-cdr-1.0.jar problem5.PlanDropRateDriver <inputPath> <outputPath>
 */
public class PlanDropRateDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: problem5.PlanDropRateDriver <inputPath> <outputPath>");
            System.err.println("Example: hadoop jar netconnect-cdr-1.0.jar problem5.PlanDropRateDriver /netconnect/cdr /netconnect/output/problem5");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NetConnect - Highest Dropped-Call Proportion by Plan");

        job.setJarByClass(PlanDropRateDriver.class);
        job.setMapperClass(PlanDropRateMapper.class);
        job.setReducerClass(PlanDropRateReducer.class);

        // 1 reducer task so all plans are compared in cleanup
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
