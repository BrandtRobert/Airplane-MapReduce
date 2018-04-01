package cs455.hadoop.mapreduce.busiestairports;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// https://stackoverflow.com/questions/31325846/finding-biggest-value-for-key

public class BusiestAirportsMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) {
		String lineSplits [] = value.toString().split(",");
		// If you got the first line in the file return, nothing can be done with it
		if (lineSplits[0].equals("Year") || lineSplits.length < 29) {
			return;
		}
		try {
			// Grab the orign and dest cities, plus the year
			Text year = new Text (lineSplits[0]);
			Text origin = new Text(lineSplits[16]);
			Text dest = new Text(lineSplits[17]);
			context.write(origin, year);
			context.write(dest, year);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
