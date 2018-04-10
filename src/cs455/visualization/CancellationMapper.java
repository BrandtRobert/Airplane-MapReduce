package cs455.visualization;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CancellationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final Map<String, String> airportToCity = new HashMap<String, String>();
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
			// Create maps from the supplementary files
			readFileIntoMap(airports, airportToCity, 0, 2, context);
			readFileIntoMap(carriers, carrierToName, 0, 1, context);
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String lineSplits [] = value.toString().split(",");
		// If you got the first line in the file return, nothing can be done with it
		if (lineSplits[0].equals("Year") || lineSplits.length < 29) {
			return;
		}
		// Year, Carrier, City, Airport, Cancelled, Cancellation Reason, Destination, Month, Day of Week
		String carrier = carrierToName.get(lineSplits[8]);
		String airport = lineSplits[16];
		String city = airportToCity.get(airport);
		String cancelled = lineSplits[21];
		String reason;
		switch (lineSplits[22]) {
			case "A":
				reason = "carrier";
				break;
			case "B":
				reason = "weather";
				break;
			case "C":
				reason = "NAS";
				break;
			case "D":
				reason = "security";
				break;
			default:
				reason = "NA";
		}
		// For counting cancellation reasons
		IntWritable cancellInt = new IntWritable(Integer.parseInt(cancelled));
		if (!reason.equals("NA")) {
			context.write(new Text("R:" + reason), cancellInt);
		}
		// For Cities / cancellations
		context.write(new Text("C:" + city), cancellInt);
		// For Airlines / cancellations
		context.write(new Text("A:" + carrier), cancellInt);
	}
}
