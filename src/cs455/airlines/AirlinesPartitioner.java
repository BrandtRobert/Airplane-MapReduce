package cs455.airlines;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AirlinesPartitioner extends Partitioner<Text, Text> {
	
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		String keyStr = key.toString();
		if (numReduceTasks == 0) {
			return 0;
		} else if (keyStr.startsWith("M1:")) {
			return 0 % numReduceTasks;
		} else if (keyStr.startsWith("W1:")) {
			return 1 % numReduceTasks;
		} else if (keyStr.startsWith("D1:")) {
			return 2 % numReduceTasks;
		} else if (keyStr.startsWith("3:")) {
			return 3 % numReduceTasks;
		} else if (keyStr.startsWith("4:")) {
			return 4 % numReduceTasks;
		} else if (keyStr.startsWith("5:")) {
			return 5 % numReduceTasks;
		} else {
			// Key starts with "6:"
			return 6 % numReduceTasks;
		}
	}

}
