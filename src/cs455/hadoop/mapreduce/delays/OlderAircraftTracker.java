package cs455.hadoop.mapreduce.delays;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class OlderAircraftTracker extends Reducer<Text, Text, Text, Text> {
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
		
		public double getAvgDelay() {
			return (double) minutesDelayed / (double) totalDelays;
		}
		
		public String getDelayStat() {
			return totalDelays + " total delays,\t" + getAvgDelay() + " avg delay";
		}
	}
	
	public void addAircraftEntry(String entryYear, String manufactureYear, String carrierDelay, String lateAircraft, Context context) {
		// DO a context write
		
		if (manufactureYear.equals("NA") || manufactureYear.equals("None")) {
			return;
		}
		int totalDelay = 0;
		if (!carrierDelay.equals("NA")) {
			totalDelay += Integer.parseInt(carrierDelay);
		}
		if (!lateAircraft.equals("NA")) {
			totalDelay += Integer.parseInt(lateAircraft);
		}
		int age = Integer.parseInt(entryYear) - Integer.parseInt(manufactureYear);
		
		if (age > 20) {
			aircraftMap.get("Old").addNewDelay(totalDelay);
		} else {
			aircraftMap.get("New").addNewDelay(totalDelay);
		}
	}
	
	public Map<String, String> getDelays() {
		Map<String, String> delayMap = new HashMap<String, String>();
		delayMap.put("Old", aircraftMap.get("Old").getDelayStat());
		delayMap.put("New", aircraftMap.get("New").getDelayStat());
		return delayMap;
	}
}
