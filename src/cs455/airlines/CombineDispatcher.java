package cs455.airlines;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombineDispatcher extends Reducer<Text, Text, Text, Text> {
	
	private AirlinesCombiner combiner = new AirlinesCombiner();

	public void reduce(Text key, Iterable<Text> values, Context context) {
		try {
			Text combinedVal = combiner.combineValues(key, values);
			context.write(key, combinedVal);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
