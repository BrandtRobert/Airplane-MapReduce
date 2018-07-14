package cs455.airlines;

import java.util.HashMap;
import java.util.Map.Entry;

public class CarrierManager {
	private HashMap<String, String> carrierToDelaysAvgDelay = new HashMap<String, String>();
	
	public void addNewCarrier(String key, String value) {
		carrierToDelaysAvgDelay.put(key, value);
	}
	
	public HashMap<String, String> returnCarriersWithAvgDelay() {
		HashMap<String, String> returnMap = new HashMap<String, String>();
		for (Entry<String, String> entry : carrierToDelaysAvgDelay.entrySet()) {
			double averageDelay = 0.0;
			String [] strArr = entry.getValue().split(",");
			int delayedMinutes = Integer.parseInt(strArr[0]);
			int totalDelays = Integer.parseInt(strArr[1]);
			averageDelay = (double) delayedMinutes / totalDelays;
			String strVal = String.format("Total Delays: %d, Delayed Minutes: %d, Avg Delay: %.2f",
					totalDelays, delayedMinutes, averageDelay);
			returnMap.put(entry.getKey(), strVal);
		}
		return returnMap;
	}
}
