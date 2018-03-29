package cs455.hadoop.mapreduce.busiestairports;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BusiestAirportsJob {
	public static void main (String args []) {
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "BusiestAiports");
			// Job
			job.setJarByClass(BusiestAirportsJob.class);
			// Mapper
			job.setMapperClass(BusiestAirportsMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			// Reducer
			job.setReducerClass(BusiestAirportsReducer.class);
			job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(3);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
