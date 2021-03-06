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
	WeatherManager weatherManger = new  WeatherManager();
	MultipleOutputs<Text, Text> mos;
	
	public void setup(Context context) {
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text combinedValue = combiner.combineValues(key, values);
		String [] keyArr = key.toString().split(":");
		if (keyArr.length != 2) {
			return;
		}
		String keyID = keyArr[0];
		Text keyTrimmed = new Text(keyArr[1]);
		switch (keyID) {
		case "M1":
			context.write(keyTrimmed, combinedValue);
		case "W1":
			context.write(keyTrimmed, combinedValue);
		case "D1":
			if (keyID.equals("D1")) context.write(keyTrimmed, combinedValue);;
			writeFinalToContext(keyTrimmed, combinedValue, context, "GeneralDelays");
			break;
		case "3":
			context.write(keyTrimmed, combinedValue);
			busiestAirports.addNewAirportRecord(keyTrimmed.toString(), combinedValue.toString());
			break;
		case "4":
			context.write(keyTrimmed, combinedValue);
			carrierManager.addNewCarrier(keyTrimmed.toString(), combinedValue.toString());
			break;
		case "5":
			context.write(keyTrimmed, combinedValue);
			writePlaneDataWithAvg(keyTrimmed.toString(), combinedValue.toString(), context);
			break;
		case "6":
			context.write(keyTrimmed, combinedValue);
			weatherManger.addWeatherDelay(keyTrimmed.toString(), combinedValue.toString());
			break;
		}
	}
	
	private void writePlaneDataWithAvg(String key, String value, Context context) {
		String [] stringArr = value.split(",");
		int numMinutes = Integer.parseInt(stringArr[0]);
		int numDelays = Integer.parseInt(stringArr[1]);
		double avgDelay = (double) numMinutes / numDelays;
		String finalValOld = String.format("Number of Delays: %d, Average Delay: %.2f", numDelays, avgDelay);
		writeFinalToContext(new Text(key), new Text(finalValOld), context, "PlaneDelays");
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
		// Write out weather data
		LinkedHashMap<Text, Text>  cityWeatherData = weatherManger.returnWritableValues();
		for (Entry<Text, Text> entry : cityWeatherData.entrySet()) {
			writeFinalToContext(entry.getKey(), entry.getValue(), context, "WeatherDelays");
		}
	}
}
