package cs455.airlines;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

public class PlaneManager {
	String oldPlaneData;
	String newPlaneData;
	
	public void addPlaneData(String key, String value) {
		if (key.equals("Old")) {
			oldPlaneData = value;
		} else {
			newPlaneData = value;
		}
	}
	
	// return (double) minutes / (double) numdelays;
	public List<Text> returnWritableDataSet() {
		if (oldPlaneData == null || newPlaneData == null) {
			return null;
		}
		List<Text> retList = new ArrayList<Text>(2);
		// Old planes
		String [] oldArr = oldPlaneData.split(",");
		int numDelaysOld = Integer.parseInt(oldArr[0]);
		int numMinutesOld  = Integer.parseInt(oldArr[1]);
		double avgDelayOld = (double) numMinutesOld / numDelaysOld;
		String finalValOld = String.format("Number of Delays: %d, Number Minutes Delayed: %d,  Average Delay: %.2f", 
				numDelaysOld, numMinutesOld, avgDelayOld);
		// New planes
		String [] newArr = newPlaneData.split(",");
		int numDelaysNew = Integer.parseInt(newArr[0]);
		int numMinutesNew  = Integer.parseInt(newArr[1]);
		double avgDelayNew = (double) numMinutesNew / numDelaysNew;
		String finalValNew = String.format("Number of Delays: %d, Number Minutes Delayed: %d, Average Delay: %.2f", 
				numDelaysNew, numMinutesNew, avgDelayNew);
		// Build list
		retList.add(new Text(finalValOld));
		retList.add(new Text(finalValNew));
		return retList;
	}
	
}
