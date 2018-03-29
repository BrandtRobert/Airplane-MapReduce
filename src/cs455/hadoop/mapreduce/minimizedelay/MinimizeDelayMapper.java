package cs455.hadoop.mapreduce.minimizedelay;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MinimizeDelayMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	// 2004,1,12,1,623,630,901,915,UA,462,N805UA,98,105,80,-14,-7,ORD,CLT,599,7,11,0,,0,0,0,0,0,0
	public void map(LongWritable key, Text value, Context context) {
		try {
			String lineSplits [] = value.toString().split(",");
			// If you got the first line in the file return, nothing can be done with it
			if (lineSplits[0].equals("Year") || lineSplits.length < 29) {
				return;
			}
			String month = String.format("M-%02d", Integer.parseInt(lineSplits[1]));
			String dayOfWeek = "D-" + lineSplits[3];
			// We will use depature time for time of day, pad to 4 places
			String timeOfDay = String.format("T-%04d", Integer.parseInt(lineSplits[5]));
			// Only use the hour
			timeOfDay = timeOfDay.substring(0,2);
			// Delays start at index 23
			int delayTime = 0;
			for (int i = 25; i < 29; i++) {
				try {
					delayTime += Integer.parseInt(lineSplits[i]);	// Some records use 'NA' instead of 0
				} catch (NumberFormatException e) {
					delayTime += 0;
				}
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