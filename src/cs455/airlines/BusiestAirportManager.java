package cs455.airlines;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

public class BusiestAirportManager {
	private HashMap<String, MutableInt> cityYearToCount = new HashMap<String, MutableInt>();
	
	public void addNewAirportRecord(String key, String count) {
		int countInt = Integer.parseInt(count);
		MutableInt value = cityYearToCount.get(key);
		if (value == null) {
			cityYearToCount.put(key, new MutableInt(countInt));
		} else {
			value.incrementBy(countInt);
		}
	}
	
	// Sorts the map into a list of top ten busiest airports by year
	// Uses the natural behavior of treemaps key sorting to achieve this
	public List<CityYearCountTriple> getTopTenByYear() {
		// Each index in the list represents a year, while the maps are sorted by counts, with string being the city
		List<TreeMap<MutableInt, String>> topTenMaps = new ArrayList<TreeMap<MutableInt, String>>(20);
		for (Entry<String, MutableInt> entry : cityYearToCount.entrySet()) {
			String [] keyArr = entry.getKey().split("-");
			String year = keyArr[0];
			String city = keyArr[1];
			int index = yearToIndex(year);
			TreeMap<MutableInt, String> yearlyMap = topTenMaps.get(index);
			yearlyMap.put(entry.getValue(), city);
			// Remove the 11th entry to get back to top ten, mostly for saving memory
			if (yearlyMap.size() == 11) {
				yearlyMap.remove(yearlyMap.firstKey());
			}
		}
		// Top 10 for 21 years
		List<CityYearCountTriple> returnList = new ArrayList<CityYearCountTriple>(10 * 21);
		// Each map is another year, subsequently
		int year = 1987;
		for (TreeMap<MutableInt, String> map : topTenMaps) {
			for (MutableInt key : map.descendingKeySet()) {
				CityYearCountTriple c = new CityYearCountTriple();
				c.city = map.get(key);
				c.count = key.toString();
				c.year = year + "";
				returnList.add(c);
			}
			year++;
		}
		return returnList;
	} 
	
	private int yearToIndex(String year) {
		int yearInt = Integer.parseInt(year);
		// 1987 is the first year of the dataset
		return yearInt - 1987;
	}
}