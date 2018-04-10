package cs455.visualization;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Mapper<LongWritable, Text, Text, IntWritable>
public class CancellationReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> cancellations, Context context) throws IOException, InterruptedException {
		for (Text val : cancellations) {
			context.write(key, val);
		}
	}
}
