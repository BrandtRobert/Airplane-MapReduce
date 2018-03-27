package cs455.hadoop.mapreduce.minimizedelay;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MinimizeDelayMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	// 2004,1,12,1,623,630,901,915,UA,462,N805UA,98,105,80,-14,-7,ORD,CLT,599,7,11,0,,0,0,0,0,0,0
	public void map(LongWritable key, Text value, Context context) {
		try {
			String lineSplits [] = value.toString().split(",");
			String month = "M-" + lineSplits[3];
			String dayOfWeek = "D-" + lineSplits[5];
			String timeOfDay = "T-" + lineSplits[6];	// We will use depature time for time of day
			// Delays start at index 23
			int delayTime = 0;
			for (int i = 25; i < 29; i++) {
				delayTime += Integer.parseInt(lineSplits[i]);
			}
			IntWritable isDelayed = (delayTime > 0) ? new IntWritable (1) : new IntWritable(0);
			context.write(new Text(month), isDelayed);
			context.write(new Text(dayOfWeek), isDelayed);
			context.write(new Text(timeOfDay), isDelayed);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
