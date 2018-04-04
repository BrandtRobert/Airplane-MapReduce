package cs455.hadoop.mapreduce.delays;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DelaysMapper extends Mapper<LongWritable, Text, Text, Text>  {
	
	private final Map<String, String> airportToCity = new HashMap<String, String>();
	private final Map<String, String> tailNumToYear = new HashMap<String, String>();
	private final Map<String, String> carrierToName = new HashMap<String, String>();
	
	private void readFileIntoMap(File input, Map<String, String> associate, int keyIndex, int valueIndex) {
		int maxIndex = Math.max(keyIndex, valueIndex);
		try {
			BufferedReader inputReader = new BufferedReader(new FileReader(input));
			String line;
			while ((line = inputReader.readLine()) != null) {
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
			File airports = new File ("/data/supplementary/airports.csv");
			File carriers = new File ("/data/supplementary/carriers.csv");
			File planeData = new File ("/data/supplementary/plane-data.csv");
			// Create maps from the supplementary files
			readFileIntoMap(airports, airportToCity, 0, 2);
			readFileIntoMap(carriers, carrierToName, 0, 1);
			readFileIntoMap(planeData, tailNumToYear, 0, 8);
		}
	}
	
	public void map(LongWritable key, Text value, Context context) {
		String lineSplits [] = value.toString().split(",");
		// If you got the first line in the file return, nothing can be done with it
		if (lineSplits[0].equals("Year") || lineSplits.length < 29) {
			return;
		}
		try {
			List<String> importantFields = new ArrayList<String>(15);
			Text origin = new Text(lineSplits[16]); // Origin
			// Associate data
			String tailNum = lineSplits[10];
			String manufactureYear = tailNumToYear.get(tailNum);
			String city = airportToCity.get(origin);
			String carrierCode = lineSplits[8];
			String carrierName = carrierToName.get(carrierCode);
			// Year, Month, DofWeek, DepAct, DepSchd, ArrAct, ArrSchd, CarrCode, CarrName, Tail#, ManuYear, City, Dest, CarrDel, WeaDel, NASDel, SecDel, LatDel
			// Times
			importantFields.add(lineSplits[0]);		// Year
			importantFields.add(lineSplits[1]);		// Month
			importantFields.add(lineSplits[3]); 	// Day of Week
			importantFields.add(lineSplits[4]); 	// Actual Departure Time
			importantFields.add(lineSplits[5]);		// Scheduled Departure time
			importantFields.add(lineSplits[6]); 	// Actual Arrival Time
			importantFields.add(lineSplits[7]); 	// Scheduled Arrival Time
			// Identifying information
			importantFields.add(carrierCode);		// Carrier Code
			importantFields.add(carrierName);		// Carrier Name
			importantFields.add(tailNum);			// Tail Num
			importantFields.add(manufactureYear);	// Aircraft Manufacture Year
			importantFields.add(city);				// Origin City
			importantFields.add(lineSplits[17]);	// Destination
			// Delays
			importantFields.add(lineSplits[24]);	// Carrier Delay
			importantFields.add(lineSplits[25]);	// Weather Delay
			importantFields.add(lineSplits[26]);	// National Air System Delay
			importantFields.add(lineSplits[27]);	// Security Delay
			importantFields.add(lineSplits[28]);	// Late Aircraft Delay
			// Combine information into a csv
			String csv = String.join(",", importantFields);
			// City, paired with info for all flights from that city
			// context.write(origin, new Text(csv));
			context.write(new Text ("Map size"), new Text(tailNumToYear.size() + ""));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
