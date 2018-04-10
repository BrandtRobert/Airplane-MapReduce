package cs455.visualization;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CancellationJob {
	public static void main (String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(CancellationJob.class);
		// Mapper
		job.setMapperClass(CancellationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// Reducer
		job.setReducerClass(CancellationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// Output Format
        // path to input in HDFS
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // path to output in HDFS
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Distributed cache files
        job.addCacheFile(new Path("/data/supplementary/airports.csv").toUri());
        job.addCacheFile(new Path("/data/supplementary/carriers.csv").toUri());
        // Block until the job is completed.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}