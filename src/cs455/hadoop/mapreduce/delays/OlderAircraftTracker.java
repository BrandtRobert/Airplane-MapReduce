package cs455.hadoop.mapreduce.delays;

import java.util.HashMap;
import java.util.Map;

public class OlderAircraftTracker {
	private final Map<String, DelayEntry> aircraftMap;
	
	public OlderAircraftTracker() {
		aircraftMap = new HashMap<String, DelayEntry>();
		aircraftMap.put("Old", new DelayEntry());
		aircraftMap.put("New", new DelayEntry());
	}
	
	private class DelayEntry {
		private int totalDelays = 0;
		private int minutesDelayed = 0;
		
		public void addNewDelay(int minutesDelayed) {
			if (minutesDelayed > 0) {
				totalDelays++;
				this.minutesDelayed += minutesDelayed;
			}
		}
	}
	
	public void addAircraftEntry(String entryYear, String manufactureYear, String carrierDelay, String lateAircraft) {
		int totalDelay = 0;
		if (isInt(carrierDelay)) {
			totalDelay += Integer.parseInt(carrierDelay);
		}
		if (isInt(lateAircraft)) {
			totalDelay += Integer.parseInt(lateAircraft);
		}
		int age = Integer.parseInt(entryYear) - Integer.parseInt(manufactureYear);
		if (age > 20) {
			aircraftMap.get("Old").addNewDelay(totalDelay);
		} else {
			aircraftMap.get("New").addNewDelay(totalDelay);
		}
	}
	
	private boolean isInt(String a) {
		try {
			Integer.parseInt(a);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}
}
