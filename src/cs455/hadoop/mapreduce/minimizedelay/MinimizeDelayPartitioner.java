package cs455.hadoop.mapreduce.minimizedelay;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

public class MinimizeDelayPartitioner extends Partitioner <Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		String keyStr = key.toString();
		if (numReduceTasks == 0) {
			return 0;
		} else if (keyStr.startsWith("M-")) {
			return 0 % numReduceTasks;
		} else if (keyStr.startsWith("D-")) {
			return 1 % numReduceTasks;
		} else {
			// Key startsWith("T-")
			return 2 % numReduceTasks;
		}
	}

}
