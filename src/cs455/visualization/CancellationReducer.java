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
		Text newKey = new Text (key.toString().substring(2));
		context.write(newKey, new IntWritable(sum));
	}
}
