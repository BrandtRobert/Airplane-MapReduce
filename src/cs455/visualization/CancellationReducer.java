package cs455.visualization;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Mapper<LongWritable, Text, Text, IntWritable>
public class CancellationReducer extends Reducer<Text, IntWritable, Text, Text> {
	public void reduce(Text key, Iterable<IntWritable> cancellations, Context context) {
		return;
	}
}
