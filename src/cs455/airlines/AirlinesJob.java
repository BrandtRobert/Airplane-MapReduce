package cs455.airlines;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AirlinesJob {
	public static void main(String args[]) {
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			
			job.setJarByClass(AirlinesJob.class);
			// Mapper
			job.setMapperClass(AirlinesMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			// Reducer
			job.setReducerClass(AirlinesReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			// Partitioner
			job.setNumReduceTasks(7);
			job.setPartitionerClass(AirlinesPartitioner.class);
			// Combiner
			job.setCombinerClass(AirlinesCombiner.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // Distributed cache files
            job.addCacheFile(new Path("/data/supplementary/airports.csv").toUri());
            job.addCacheFile(new Path("/data/supplementary/carriers.csv").toUri());
            job.addCacheFile(new Path("/data/supplementary/plane-data.csv").toUri());
            // Multiple outputs
            MultipleOutputs.addNamedOutput(job, "GeneralDelays", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "MajorHubs", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "CarrierDelays", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "PlaneDelays", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "WeatherDelays", TextOutputFormat.class, Text.class, Text.class);
            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
