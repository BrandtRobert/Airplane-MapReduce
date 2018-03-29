package cs455.hadoop.mapreduce.minimizedelay;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MinimizeDelayReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
	public void reduce (Text key, Iterable<IntWritable> delays, Context context) {
		try {
			int count = 0;
			for (IntWritable val : delays) {
				count += val.get();
			}
			// Trim off the M-, D-, or T- prefix
			Text trimKey = new Text(key.toString().substring(2));
			context.write(trimKey, new IntWritable(count));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
