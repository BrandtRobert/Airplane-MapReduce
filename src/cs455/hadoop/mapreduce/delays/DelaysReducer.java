package cs455.hadoop.mapreduce.delays;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DelaysReducer extends Reducer<Text, Text, Text, Text> {
	private final CarrierDelayTracker carrierTracker = new CarrierDelayTracker();
	private final OlderAircraftTracker aircraftTracker = new OlderAircraftTracker();
	// Year-0, Month-1, DofWeek-2, DepAct-3, DepSchd-4, ArrAct-5, ArrSchd-6,
	// CarrCode-7, CarrName-8, Tail#-9, ManuYear-10, City-11, Dest-12, CarrDel-13, 
	// WeaDel-14, NASDel-15, SecDel-16, LatDel-17
	public void reduce (Text airportCode, Iterable<Text> flightRecords, Context context) {
		for (Text record : flightRecords) {
			String[] splits = record.toString().split(",");
			trackCarriers(splits);
			trackAircraft(splits, context);
		}
	}
	
	private void trackAircraft(String[] flightRecord, Context context) {
		String entryYear = flightRecord[0];
		String manufactureYear = flightRecord[10];
		String carrierDelay = flightRecord[13];
		String lateDelay = flightRecord[17];
		aircraftTracker.addAircraftEntry(entryYear, manufactureYear, carrierDelay, lateDelay, context);
	}

	private void trackCarriers(String[] flightRecord) {
		int minutesDelayed;
		try {
			// Prefer using the carrier delay field, if not available calculate arrival delay
			minutesDelayed = Integer.parseInt(flightRecord[13]);
			// if (!splits[13].equals("NA")) {
			// 	minutesDelayed = Integer.parseInt(flightRecord[13]);
			// } else {
			// 	minutesDelayed = Integer.parseInt(flightRecord[5]) - Integer.parseInt(flightRecord[6]);
			// 	// If the plane arrived before scheluded count it as 0 delay
			// 	minutesDelayed = Math.max(minutesDelayed, 0);
			// }
		} catch (NumberFormatException e) {
			minutesDelayed = 0;
		}
		carrierTracker.addDelay(flightRecord[8], minutesDelayed);
}

	public void cleanup(Context context) {
		MultipleOutputs<Text, Text> mos = new MultipleOutputs<Text, Text>(context);
		try {
			Map<String, String> delayStats = carrierTracker.getDelayStats();
			for (Entry<String, String> entry : delayStats.entrySet()) {
				// This isn't writing for some reason
				mos.write("CarriersOutput", new Text(entry.getKey()), new Text (entry.getValue()));
				context.write(new Text(entry.getKey()), new Text (entry.getValue()));
			}
			Map<String, String> aircraftStats = aircraftTracker.getDelays();
			for (Entry<String, String> entry : aircraftStats.entrySet()) {
				mos.write("AircraftOutput", new Text(entry.getKey()), new Text (entry.getValue()));
			}
			mos.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
