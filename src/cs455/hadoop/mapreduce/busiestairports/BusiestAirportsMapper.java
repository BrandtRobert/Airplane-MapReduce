package cs455.hadoop.mapreduce.busiestairports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// https://stackoverflow.com/questions/31325846/finding-biggest-value-for-key

public class BusiestAirportsMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) {
		
	}
}
