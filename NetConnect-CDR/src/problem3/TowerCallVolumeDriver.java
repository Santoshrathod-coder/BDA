package problem3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * NetConnect CDR Analysis - Problem 3: Tower Call Volume Driver
 *
 * Usage:
 *   hadoop jar netconnect-cdr-1.0.jar problem3.TowerCallVolumeDriver <inputPath> <outputPath>
 */
public class TowerCallVolumeDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: problem3.TowerCallVolumeDriver <inputPath> <outputPath>");
            System.err.println("Example: hadoop jar netconnect-cdr-1.0.jar problem3.TowerCallVolumeDriver /netconnect/cdr /netconnect/output/problem3");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NetConnect - Tower Call Volume Analysis");

        job.setJarByClass(TowerCallVolumeDriver.class);
        job.setMapperClass(TowerCallVolumeMapper.class);
        job.setCombinerClass(TowerCallVolumeReducer.class);
        job.setReducerClass(TowerCallVolumeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
