package cs455.hadoop.mapreduce.delays;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DelaysReducer extends Reducer<Text, Text, Text, Text> {
	private final CarrierDelayTracker carrierTracker = new CarrierDelayTracker();
    // Year-0, Month-1, DofWeek-2, DepAct-3, DepSchd-4, ArrAct-5, ArrSchd-6,
    // CarrCode-7, CarrName-8, Tail#-8, ManuYear-9, City-10, Dest-11, CarrDel-12, 
    // WeaDel-13, NASDel-14, SecDel-15, LatDel-16
	public void reduce (Text airportCode, Iterable<Text> flightRecords, Context context) {
		for (Text record : flightRecords) {
			String [] splits = record.toString().split(",");
			int minutesDelayed = (splits[12].equals("NA")) ? 0 : Integer.parseInt(splits[12]);
			carrierTracker.addDelay(splits[8], minutesDelayed);
		}
	}
	
	public void cleanUp(Context context) {
		try {
			Map<String, String> delayStats = carrierTracker.getDelayStats();
			for (Entry<String, String> entry : delayStats.entrySet()) {
				context.write(new Text(entry.getKey()), new Text (entry.getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
