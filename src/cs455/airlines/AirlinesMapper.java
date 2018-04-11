package cs455.airlines;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;

public class AirlinesMapper extends Mapper<LongWritable, Text, Text, Text> {

	private final Map<String, String> airportToCity = new HashMap<String, String>();
	private final Map<String, String> tailNumToYear = new HashMap<String, String>();
	private final Map<String, String> carrierToName = new HashMap<String, String>();
	
	private void readFileIntoMap(Path path, Map<String, String> associate, int keyIndex, int valueIndex, Context context) {
		int maxIndex = Math.max(keyIndex, valueIndex);
		try {
			// Retrieve the hadoop file system in order to open the file
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader inputReader = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			while ((line = inputReader.readLine()) != null) {
				// Remove quotes
				line = line.replaceAll("\"", "");
				String [] splits = line.split(",");
				if (splits.length < maxIndex) {
					continue;
				}
				associate.put(splits[keyIndex], splits[valueIndex]);
			}
			inputReader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// Cache contains: airports.csv, carriers.csv, plane-data.csv 
	public void setup(Context context) throws IOException {
		if (context.getCacheFiles().length > 0) {
			URI [] paths = context.getCacheFiles();
			Path airports = new Path (paths[0]);
			Path carriers = new Path (paths[1]);
			Path planeData = new Path (paths[2]);
			// Create maps from the supplementary files
			readFileIntoMap(airports, airportToCity, 0, 2, context);
			readFileIntoMap(carriers, carrierToName, 0, 1, context);
			readFileIntoMap(planeData, tailNumToYear, 0, 8, context);
		}
	}
	
	//Q1 & Q2 worst and best times for delays
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String lineSplits [] = value.toString().split(",");
		// If you got the first line in the file return, nothing can be done with it
		if (lineSplits[0].equals("Year") || lineSplits.length < 29) {
			return;
		}
		// Dept time
		String year = lineSplits[0];
		String month = lineSplits[1];
		String dayOfWeek = lineSplits[3];
		String deptTimeSchd = lineSplits[5];
		// Calculate timeOfDay
		String timeOfDay = String.format("T-%04d", Integer.parseInt(lineSplits[5]));
		// Only use the hour
		timeOfDay = timeOfDay.substring(0,4);
		// midnight '24' and hour '00' are the same thing
		if (timeOfDay.equals("T-24")) {
			timeOfDay = "T-00";
		}
		// Calculate general delay
		int deptTimeSchdInt = Integer.parseInt(deptTimeSchd);
		int deptTimeInt = (isAvailable(lineSplits[4])) ? Integer.parseInt(lineSplits[4]) : deptTimeSchdInt;
		int arrTimeSchdInt = Integer.parseInt(lineSplits[7]);
		int arrTimeInt = (isAvailable(lineSplits[6])) ? Integer.parseInt(lineSplits[6]) : arrTimeSchdInt;
		
		String delayed = "0";
		// If both are 0, will be 0
		int maxDelay = Math.max(Math.max((deptTimeInt - deptTimeSchdInt), (arrTimeInt - arrTimeSchdInt)), 0);
		if (maxDelay > 0) {
			delayed = "1";
		}
		// Write data for Q1 & Q2 (best/worst times to fly)
		context.write(new Text("M1:" + month), new Text(delayed));
		context.write(new Text("W1:" + dayOfWeek), new Text(delayed));
		context.write(new Text("D1:" + timeOfDay), new Text(delayed));
		// Q3 which airports are busiest by year
		String originAirport = lineSplits[16];
		String destAirport = lineSplits[17];
		context.write(new Text("3:" + year + "-" + originAirport), new Text("1"));
		context.write(new Text("3:" + year + "-" + destAirport), new Text("1"));
		// Q4 which carriers have the most delays, and average delay
		String carrierCode = lineSplits[8];
		String carrierFull = carrierToName.get(carrierCode);
		String carrierDelay = lineSplits[24];
		if (!carrierDelay.equals("NA") && !carrierDelay.equals("0")) {
			context.write(new Text("4:" + carrierFull), new Text(carrierDelay + ",1"));
		}
		// Q5 do older planes cause more delays?
		String tailNum = lineSplits[10];
		String manufactureYear = (tailNumToYear.containsKey(tailNum)) ? tailNumToYear.get(tailNum) : "NA";
		if (!manufactureYear.equals("NA") && !manufactureYear.equals("None") && !delayed.equals("0")) {
			int manuYearInt = Integer.parseInt(manufactureYear);
			int yearInt = Integer.parseInt(year);
			int aircraftAge = yearInt - manuYearInt;
			if (aircraftAge > 20) {
				context.write(new Text("5:Old"), new Text(maxDelay + "," + delayed));
			} else {
				context.write(new Text("5:New"), new Text(maxDelay + "," + delayed));
			}
		}
		// Q6 which cities have the most weather delays?
		String weatherDelay = lineSplits[25];
		if (!weatherDelay.equals("NA")) {
			String cityOrg = airportToCity.get(originAirport);
			String cityDest = airportToCity.get(destAirport);
			context.write(new Text("6:" + cityOrg), new Text("1"));
			if (cityDest != null) {
				context.write(new Text("6:" + cityDest), new Text("1"));
			}
		}
	}
	
	private boolean isAvailable(String field) {
		return !(field.equals("NA") || field.equals(""));
	}
}
