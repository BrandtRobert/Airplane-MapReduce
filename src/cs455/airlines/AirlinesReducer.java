package cs455.airlines;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class AirlinesReducer extends Reducer<Text, Text, Text, Text> {
	
	AirlinesCombiner combiner = new AirlinesCombiner();
	BusiestAirportManager busiestAirports = new BusiestAirportManager();
	CarrierManager carrierManager = new CarrierManager();
	PlaneManager planeManager = new PlaneManager();
	WeatherManager weatherManger = new  WeatherManager();
	MultipleOutputs<Text, Text> mos;
	
	public void setup(Context context) {
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) {
		Text combinedValue = combiner.combineValues(key, values);
		String [] keyArr = key.toString().split(":");
		String keyID = keyArr[0];
		Text keyTrimmed = new Text(keyArr[1]);
		switch (keyID) {
		case "M1:":
		case "W1:":
		case "D1:":
			writeFinalToContext(keyTrimmed, combinedValue, context, "GeneralDelays");
			break;
		case "3:":
			busiestAirports.addNewAirportRecord(keyTrimmed.toString(), combinedValue.toString());
			break;
		case "4:":
			carrierManager.addNewCarrier(keyTrimmed.toString(), combinedValue.toString());
			break;
		case "5:":
			planeManager.addPlaneData(keyTrimmed.toString(), combinedValue.toString());
			break;
		case "6:":
			weatherManger.addWeatherDelay(keyTrimmed.toString(), combinedValue.toString());
			break;
		}
	}
	
	private void writeFinalToContext(Text key, Text value, Context context, String namedOutput) {
		try {
			mos.write(namedOutput, key, value);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void cleanup(Context context) {
		Text key = new Text();
		Text value = new Text();
		// Write the top ten busiest airports for each year
		List<CityYearCountTriple> busiestAirportList = busiestAirports.getTopTenByYear();
		for (CityYearCountTriple c : busiestAirportList) {
			key.set(c.year + " " + c.city);
			value.set(c.count);
			writeFinalToContext(key, value, context, "MajorHubs");
		}
		// Write out carriers and their delay counts
		HashMap<String, String> carrierMap = carrierManager.returnCarriersWithAvgDelay();
		for (Entry<String, String> entry : carrierMap.entrySet()) {
			key.set(entry.getKey());
			value.set(entry.getValue());
			writeFinalToContext(key, value, context, "CarrierDelays");
		}
		// Write out old and new plane data
		List<Text> planeData =  planeManager.returnWritableDataSet();
		writeFinalToContext(new Text("Old Planes"), planeData.get(0), context, "PlaneDelays");
		writeFinalToContext(new Text("New Planes"), planeData.get(1), context, "PlaneDelays");
		// Write out weather data
		LinkedHashMap<Text, Text>  cityWeatherData = weatherManger.returnWritableValues();
		for (Entry<Text, Text> entry : cityWeatherData.entrySet()) {
			writeFinalToContext(entry.getKey(), entry.getValue(), context, "WeatherDelays");
		}
	}
}
