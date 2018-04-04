package cs455.hadoop.mapreduce.delays;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CarrierDelayTracker {
	private Map<String, CarrierEntry> carriers = new HashMap<String, CarrierEntry>();
	
	private class CarrierEntry {
		private Long minutes = 0L;
		private int numdelays = 0;
		void addDelay(int minutes) {
			numdelays++;
			this.minutes += minutes;
		}
		double avgDelay() {
			return (double) minutes / (double) numdelays;
		}
	}
	
	public void addDelay(String carrier, int minutes) {
		if (carriers.containsKey(carrier)) {
			carriers.get(carrier).addDelay(minutes);
		} else {
			CarrierEntry newEntry = new CarrierEntry();
			newEntry.addDelay(minutes);
			carriers.put(carrier, newEntry);
		}
	}
	
	public Map<String, String> getDelayStats() {
		Map<String, String> averages = new HashMap<String, String>();
		for (Entry<String, CarrierEntry> entry : carriers.entrySet()) {
			CarrierEntry c = entry.getValue();
			String stats = String.format("%d delays\t%d minutes\t%.2f average delay", c.numdelays, c.minutes, c.avgDelay());
			averages.put(entry.getKey(), stats);
		}
		return averages;
	}
}
