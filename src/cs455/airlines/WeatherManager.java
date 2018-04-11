package cs455.airlines;

import java.util.LinkedHashMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;

public class WeatherManager {
	private TreeMap<MutableInt, String> delaysToCity = new TreeMap<MutableInt, String>();
	
	public void addWeatherDelay(String key, String value) {
		int count = Integer.parseInt(value);
		MutableInt val = new MutableInt(count);
		delaysToCity.put(val, key);
		// Don't let it grow over 11 entries
		if (delaysToCity.size() == 11) {
			delaysToCity.remove(delaysToCity.firstKey());
		}
	}
	
	public LinkedHashMap<Text, Text> returnWritableValues() {
		LinkedHashMap<Text, Text> returnSet = new LinkedHashMap<Text, Text>();
		// Flip the keys and values around
		for (MutableInt key : delaysToCity.descendingKeySet()) {
			Text city = new Text(delaysToCity.get(key));
			Text count = new Text(key.toString());
			returnSet.put(city, count);
		}
		return returnSet;
	}
}
