package cs455.visualization;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Mapper<LongWritable, Text, Text, IntWritable>
public class CancellationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> cancellations, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable can : cancellations) {
			sum += can.get();
		}
		String [] parts = key.toString().split(":");
		Text newKey = new Text(parts[parts.length -1]);
		context.write(newKey, new IntWritable(sum));
	}
}
