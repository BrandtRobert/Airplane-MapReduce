package cs455.visualization;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CancellationPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable val, int numReduceTasks) {
		String keyStr = key.toString();
		if (numReduceTasks == 0) {
			return 0;
		} else if (keyStr.startsWith("D:")) {
			return 0 % numReduceTasks;
		} else if (keyStr.startsWith("C:")) {
			return 1 % numReduceTasks;
		} else { // Key starts with A:
			return 2 % numReduceTasks;
		}
	}

}
